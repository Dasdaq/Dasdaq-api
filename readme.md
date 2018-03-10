
## 启动worker
`celery -A app.celery worker`

## 启动定时任务
`celery beat -A app.celery`

## 启动api服务
`python app.py`