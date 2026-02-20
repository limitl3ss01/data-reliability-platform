from drp.config.settings import Settings


def test_duckdb_path_reads_from_env(monkeypatch) -> None:
    monkeypatch.setenv("DUCKDB_PATH", "/tmp/test-warehouse.duckdb")
    settings = Settings()
    assert settings.duckdb_path == "/tmp/test-warehouse.duckdb"
