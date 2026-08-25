import random

def calculate_backoff(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
    # Calculate exponential delay
    delay = min(max_delay, base_delay * (2 ** attempt))
    # Apply full jitter
    jittered_delay = random.uniform(0, delay)
    return jittered_delay

# Example retry delays across attempts:
for attempt in range(4):
    print(f"Attempt {attempt + 1} backoff: {calculate_backoff(attempt):.2f}s")