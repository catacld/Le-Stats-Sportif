This Flask-based API server efficiently processes requests for U.S. health statistics (nutrition, physical activity, obesity) from a large CSV dataset. Key features include:

Multi-Threaded: Handles concurrent requests to enhance performance.<br>
Asynchronous Processing: Queues requests, returns job IDs, and provides results upon completion.<br>
Comprehensive API Endpoints: Offers diverse statistical calculations (means, comparisons, rankings, category breakdowns) across states and time periods.<br>
Graceful Shutdown: Ensures all queued jobs are completed before shutting down.<br>
Extensive Testing: Includes unit and functional tests to ensure reliability.<br>
Detailed Logging: Provides comprehensive logs for debugging and monitoring.
