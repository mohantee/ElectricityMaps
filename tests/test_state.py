from electricity_maps.utils.state import PipelineState


def test_state_init_layer(test_settings):
    """Positive scenario: Initialize a layer in el_state."""
    state = PipelineState(test_settings)
    process_ts = 1745489400

    state.init_layer("bronze", process_ts)

    df = state.get_state_summary()
    assert len(df) == 1
    assert df[0, "process"] == "bronze"
    assert df[0, "process_ts"] == process_ts
    assert df[0, "status"] == "I"

def test_state_mark_ready(test_settings):
    """Positive scenario: Mark a layer as ready."""
    state = PipelineState(test_settings)
    process_ts = 1745489400
    state.init_layer("bronze", process_ts)

    state.mark_ready("bronze", process_ts, record_count=100)

    df = state.get_state_summary()
    assert df[0, "status"] == "R"
    assert df[0, "record_count"] == 100
    assert df[0, "end_timestamp"] is not None

def test_state_pickup_ready(test_settings):
    """Positive scenario: Pickup ready rows and flip to P."""
    state = PipelineState(test_settings)
    process_ts = 1745489400
    state.init_layer("bronze", process_ts)
    state.mark_ready("bronze", process_ts, record_count=100)

    pending = state.pickup_ready("bronze")

    assert pending == [process_ts]
    df = state.get_state_summary()
    assert df[0, "status"] == "P"

def test_state_empty_pickup(test_settings):
    """Negative scenario: Pickup when no rows are ready."""
    state = PipelineState(test_settings)
    pending = state.pickup_ready("bronze")
    assert pending == []

def test_state_mark_error(test_settings):
    """Negative scenario: Record an error in el_state."""
    state = PipelineState(test_settings)
    process_ts = 1745489400
    state.init_layer("bronze", process_ts)

    state.mark_error("bronze", process_ts, "API connection failed")

    df = state.get_state_summary()
    assert df[0, "status"] == "I"
    assert df[0, "error_message"] == "API connection failed"
