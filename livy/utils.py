def polling_intervals(
    start, rest, max_duration = None
):
    def _intervals():
        for i in start:
            yield i

        while True:
            yield rest

    cumulative = 0.0
    for interval in _intervals():
        cumulative += interval
        if max_duration is not None and cumulative > max_duration:
            break
        yield interval
