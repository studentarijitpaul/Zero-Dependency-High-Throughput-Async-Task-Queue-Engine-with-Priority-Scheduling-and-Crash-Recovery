import asyncio
import os
import smtplib
from email.message import EmailMessage

from async_task_queue.queue.queue import TaskQueue
from async_task_queue.task.registry import TaskRegistry


async def send_email(payload: dict[str, object]) -> None:
    """Send an email using Gmail SMTP."""
    username = os.environ["EMAIL_USERNAME"]
    app_password = os.environ["EMAIL_APP_PASSWORD"]

    message = EmailMessage()
    message["From"] = username
    message["To"] = str(payload["to"])
    message["Subject"] = str(payload["subject"])
    message.set_content(str(payload["body"]))

    print(f"Connecting to Gmail as {username}...")

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
            smtp.login(username, app_password)
            print("Gmail authentication successful.")

            smtp.send_message(message)

        print(f"Email sent successfully to {payload['to']}.")

    except Exception as exception:
        print(f"Email sending failed: {type(exception).__name__}: {exception}")
        raise


async def main() -> None:
    """Run the Good Morning email through the task queue."""
    registry = TaskRegistry()

    registry.register(
        "send_email",
        send_email,
    )

    queue = TaskQueue(
        registry=registry,
        worker_count=1,
    )

    await queue.start()

    try:
        task_id = await queue.submit(
            "send_email",
            payload={
                "to": "ap.dev.official@gmail.com",
                "subject": "Good Morning! Rise and Shine 🌟",
                "body": (
                    "Good morning, Arijit!\n\n"
                    "Today is a brand-new day filled with fresh "
                    "opportunities. Keep pushing forward, stay focused "
                    "on your goals, and give it your best effort today. "
                    "You've got what it takes to achieve great things!\n\n"
                    "Make today count!"
                ),
            },
            priority=0,
            max_retries=3,
        )

        print(f"Email task submitted: {task_id}")

        await queue.wait(task_id)

        task = queue.get(task_id)

        print(f"Final task status: {task.status}")

    finally:
        await queue.stop()


if __name__ == "__main__":
    asyncio.run(main())
