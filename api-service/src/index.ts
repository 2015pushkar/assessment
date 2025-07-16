import app from './app';

const PORT = process.env.PORT || 3000;

async function startServer() {
  try {
    const server = await app();
    
    server.listen(PORT, () => {
      console.log(`🚀 Clinical Data API Server running on port ${PORT}`);
      console.log(`📊 ETL Service URL: ${process.env.ETL_SERVICE_URL}`);
      console.log(`🗄️  Database URL: ${process.env.DATABASE_URL}`);
    });
  } catch (error) {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
  }
}

startServer();
