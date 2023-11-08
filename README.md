Link to the project https://github.com/dkarpele/graduate_work

# Graduate work

### Installation

1. Clone [repo](https://github.com/dkarpele/graduate_work).
2. Create ```.env``` file according to ```.env.example```.
3. Create ```.env.minio.json``` file according to ```.env.minio.json.example```.
4. Launch the project ```docker-compose up --build```.


#### [architecture](architecture)

Architecture for the CDN service described here.

#### [CDN](cdn_api_async_redis)

We use MinIO as a S3 and redis as a cache. 

**API**

- GET http://127.0.0.1/api/v1/films/{object_name} - Get URL to preview object (movie, music, photo). Redirects to url to preview object
- GET http://127.0.0.1/api/v1/films/{object_name}/status - Get status of the object uploading to S3
- POST http://127.0.0.1/api/v1/films/upload_object - upload object to storage
- DELETE http://127.0.0.1/api/v1/films/delete_object - delete object from alll nodes

**Scheduler**

- finish_in_progress_tasks. If the task failed during last 6 hours, scheduler finish uploading
- abort_old_tasks. The task will remove all in_progress tasks older than 6 hours

**Redis**

After every action the transition status in the database changes:

in_progress -> finished.

- in_progress - part of the file was uploaded to S3
- finished - upload finished
