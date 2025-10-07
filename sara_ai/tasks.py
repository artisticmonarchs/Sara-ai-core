from sara_ai.celery_app import celery_app
from sara_ai.logging_utils import log_event

@celery_app.task(name="example_task")
def example_task(data: dict, trace_id: str | None = None):
    trace_id = log_event(
        service="celery",
        event="task_start",
        status="ok",
        message=f"Task received: {data}",
        trace_id=trace_id,
    )
    # Business logic here
    log_event(
        service="celery",
        event="task_complete",
        status="ok",
        message=f"Task completed: {data}",
        trace_id=trace_id,
    )
    return {"status": "ok", "trace_id": trace_id}
