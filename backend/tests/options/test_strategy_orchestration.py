from types import SimpleNamespace

from options.strategy_orchestration import OptionStrategyPipeline


class FakeAnalysisRepository:
    def __init__(self):
        self.cycle_call = None

    def list_latest_complete_cycle(self, underlyers, context, policy_sha256):
        self.cycle_call = (underlyers, context, policy_sha256)
        return (
            SimpleNamespace(underlyer="AAPL"),
            SimpleNamespace(underlyer="SPY"),
        )

    def get_latest(self, underlyer, context):
        raise AssertionError("full-universe replay must use one coherent cycle")


def test_full_universe_strategy_replay_uses_one_complete_cycle():
    analysis_repository = FakeAnalysisRepository()
    pipeline = object.__new__(OptionStrategyPipeline)
    pipeline.configuration = SimpleNamespace(
        policy_sha256="a" * 64,
        settings=SimpleNamespace(
            underlyers=("AAPL", "SPY"),
            fixed_etf_underlyers=("SPY",),
        ),
    )
    pipeline.analysis_repository = analysis_repository
    processed = []
    pipeline.process_persisted = lambda run, asset_type: processed.append(
        (run.underlyer, asset_type.value)
    ) or run.underlyer

    assert pipeline.run_latest() == ("AAPL", "SPY")
    assert analysis_repository.cycle_call[0] == ("AAPL", "SPY")
    assert analysis_repository.cycle_call[2] == "a" * 64
    assert processed == [("AAPL", "STOCK"), ("SPY", "ETF")]