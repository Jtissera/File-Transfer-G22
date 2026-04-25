
from protocol import DEFAULT_TIMEOUT


_ALPHA = 0.125   # 1/8 
_BETA = 0.25     # 1/4
_MAX_RTO = 60.0  # Pa evitar esperas muy largas

def rtt_new(initial_rtt = DEFAULT_TIMEOUT):
    
    return {
        "estimatedRTT": initial_rtt,
        "devRTT": initial_rtt / 2,
    }


def rtt_update(state, sample_rtt):

    estimated =  _ALPHA * sample_rtt + (1 - _ALPHA) * state["estimatedRTT"] 
    dev = _BETA * abs(sample_rtt - estimated) + (1 - _BETA) * state["devRTT"] 
    
    return {"estimatedRTT": estimated, "devRTT": dev}


def rtt_timeout(state):

    rto = state["estimatedRTT"] + 4 * state["devRTT"]

    return min(_MAX_RTO, rto)

def rtt_duplicate(state):
    estimated = min(state["estimatedRTT"] * 2, _MAX_RTO)

    return {"estimatedRTT": estimated, "devRTT":  state["devRTT"]}