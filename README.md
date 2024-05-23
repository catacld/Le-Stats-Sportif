This Flask-based API server efficiently processes requests for U.S. health statistics (nutrition, physical activity, obesity) from a large CSV dataset. Key features include:

Multi-Threaded: Handles concurrent requests to enhance performance.
Asynchronous Processing: Queues requests, returns job IDs, and provides results upon completion.
Comprehensive API Endpoints: Offers diverse statistical calculations (means, comparisons, rankings, category breakdowns) across states and time periods.
Graceful Shutdown: Ensures all queued jobs are completed before shutting down.
Extensive Testing: Includes unit and functional tests to ensure reliability.
Detailed Logging: Provides comprehensive logs for debugging and monitoring.
