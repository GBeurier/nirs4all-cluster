"""Real SDK replay closure, including float64 ArrayStore consumer input."""

import json
from pathlib import Path

import pytest

nirs4all = pytest.importorskip("nirs4all")
np = pytest.importorskip("numpy")

from nirs4all.pipeline.storage.workspace_store import WorkspaceStore  # noqa: E402

from nirs4all_cluster.runners import nirs4all_run as runner  # noqa: E402


@pytest.mark.parametrize("engine", ["legacy", "dag-ml"])
def test_published_record_bundle_replays_persisted_arrays_without_fit(tmp_path, monkeypatch, engine):
    from sklearn.cross_decomposition import PLSRegression

    dataset = tmp_path / "dataset"
    dataset.mkdir()
    rng = np.random.default_rng(42)
    X = rng.normal(size=(48, 12))
    y = X[:, 0] * 2 + X[:, 3] - X[:, 7]
    for partition, selected in [("cal", slice(0, 36)), ("val", slice(36, 48))]:
        np.savetxt(
            dataset / f"X{partition}.csv",
            X[selected],
            delimiter=";",
            header=";".join(str(1100 + 5 * i) for i in range(12)),
            comments="",
        )
        np.savetxt(dataset / f"Y{partition}.csv", y[selected], delimiter=";", header="target", comments="")
    pipeline = tmp_path / "pipeline.yaml"
    pipeline.write_text(
        "pipeline:\n  - class: sklearn.model_selection.KFold\n    params:\n"
        "      n_splits: 3\n      shuffle: true\n      random_state: 42\n"
        "  - class: sklearn.preprocessing.StandardScaler\n"
        "  - class: sklearn.cross_decomposition.PLSRegression\n    params:\n      n_components: 3\n"
    )
    task = tmp_path / "task.json"
    task.write_text(
        json.dumps(
            {
                "pipeline": {"mode": "path", "path": str(pipeline)},
                "dataset": {"mode": "path", "path": str(dataset)},
                "params": {"engine": engine, "random_state": 42, "refit": True},
                "outputs": {"export_best_model": True, "keep_task_workspace": False},
                "native_payload": {
                    "manifest": {
                        "robustnessEvidencePublicationHandoff": {
                            "kind": "robustness_evidence_publication_handoff",
                            "requested": True,
                            "destination": "result_metadata.robustness_evidence",
                            "failClosed": True,
                            "publishedFields": [
                                "prediction_arrays.X",
                                "result_metadata.robustness_evidence.X",
                                "result_metadata.robustness_evidence.predictor_bundle",
                            ],
                            "alignmentStrategies": ["sample_indices"],
                        }
                    }
                },
            }
        )
    )
    workspace = tmp_path / "workspace"
    result_file = tmp_path / "result.json"
    assert (
        runner.main(
            [
                "--task-file",
                str(task),
                "--workspace",
                str(workspace),
                "--output-dir",
                str(tmp_path / "outputs"),
                "--result-file",
                str(result_file),
            ]
        )
        == 0
    )

    def forbidden_fit(*args, **kwargs):
        raise AssertionError("Cold replay must not fit")

    monkeypatch.setattr(PLSRegression, "fit", forbidden_fit)
    store = WorkspaceStore(workspace)
    folds_replayed = set()
    final_replayed = 0
    try:
        rows = list(store.query_predictions().iter_rows(named=True))
        assert len(rows) == 17
        for row in rows:
            arrays = store.array_store.load_single(row["prediction_id"], dataset_name=row["dataset_name"])
            assert arrays["X"].dtype == np.float64
            evidence = arrays["result_metadata"]["robustness_evidence"]
            bundle = evidence.get("predictor_bundle")
            if bundle:
                assert Path(bundle).is_file()
                prediction = nirs4all.predict(
                    model=bundle,
                    data={"X": arrays["X"]},
                    engine=engine,
                    workspace_path=str(tmp_path / "cold-replay"),
                    verbose=0,
                )
                np.testing.assert_array_equal(
                    np.asarray(prediction.y_pred).reshape(-1), np.asarray(arrays["y_pred"]).reshape(-1)
                )
                if row["fold_id"] == "final":
                    final_replayed += 1
                elif str(row["fold_id"]).isdecimal():
                    folds_replayed.add(str(row["fold_id"]))
            else:
                assert evidence["publication"]["status"] != "published"
                assert "predictor_bundle" in evidence["publication"]["missing"]
        assert final_replayed == 2
        assert folds_replayed == ({"0", "1", "2"} if engine == "legacy" else set())
    finally:
        store.close()
