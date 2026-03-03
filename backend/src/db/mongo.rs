use mongodb::{bson::doc, options::ClientOptions, Client, Collection, Database};
use tracing::{info, instrument};

use crate::config::MongoDbConfig;
use crate::error::AppError;
use crate::model::script::ScriptDocument;

/// MongoDB client wrapper for querying script documents.
#[derive(Clone)]
pub struct MongoRepo {
    database: Database,
    collection: Collection<ScriptDocument>,
}

impl MongoRepo {
    /// Initialize MongoDB connection and return a repo handle.
    pub async fn connect(config: &MongoDbConfig) -> Result<Self, AppError> {
        let client_options = ClientOptions::parse(&config.uri).await?;
        let client = Client::with_options(client_options)?;

        // Verify connectivity
        let database = client.database(&config.database);
        database.run_command(doc! { "ping": 1 }, None).await?;
        let collection = database.collection::<ScriptDocument>(&config.collection);

        info!(
            database = %config.database,
            collection = %config.collection,
            "Connected to MongoDB"
        );

        Ok(Self {
            database,
            collection,
        })
    }

    /// Query the script document for a given task_id.
    #[instrument(skip(self), fields(task_id))]
    pub async fn find_script_by_task_id(&self, task_id: i64) -> Result<ScriptDocument, AppError> {
        let filter = doc! { "task_id": task_id };

        let doc = self
            .collection
            .find_one(filter, None)
            .await?
            .ok_or_else(|| {
                AppError::TaskFailed(format!("Script not found for task_id={task_id}"))
            })?;

        info!(
            task_id,
            scenes = doc.scenes.len(),
            "Fetched script document"
        );

        Ok(doc)
    }

    /// Ping MongoDB for health checks.
    pub async fn ping(&self) -> Result<(), AppError> {
        self.database.run_command(doc! { "ping": 1 }, None).await?;
        Ok(())
    }
}
