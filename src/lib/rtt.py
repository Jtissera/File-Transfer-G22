from constants import DEFAULT_TIMEOUT, ALPHA, BETA, MAX_RTO


def rtt_new(initial_rtt=DEFAULT_TIMEOUT):

    return {
        "estimatedRTT": initial_rtt,
        "devRTT": initial_rtt / 2,
    }


def rtt_update(state, sample_rtt):

    estimated = ALPHA * sample_rtt + (1 - ALPHA) * state["estimatedRTT"]
    dev = BETA * abs(sample_rtt - estimated) + (1 - BETA) * state["devRTT"]

    return {"estimatedRTT": estimated, "devRTT": dev}


def rtt_timeout(state):

    rto = state["estimatedRTT"] + 4 * state["devRTT"]

    return min(MAX_RTO, rto)


def rtt_duplicate(state):
    estimated = min(state["estimatedRTT"] * 2, MAX_RTO)

    return {"estimatedRTT": estimated, "devRTT": state["devRTT"]}
