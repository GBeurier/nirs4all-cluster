"""Unit coverage for the additive DAG scheduler-shape inference.

``JobRequest.inferred_scheduler_contract()`` is what the server attests as the
job ``scheduler.shape`` (``server/app.py`` re-infers it and ignores any client
claim — see ``tests/test_rbac.py::test_scheduler_shape_is_server_normalized``).
The RBAC/e2e suites only touch this through the ``dagml`` form; these tests pin
the whole classifier: the ``dag`` key, ``after``/``deps`` step lists, the plain
linear pipeline, the matrix, and the DAG-beats-matrix precedence.

No nirs4all is imported (schema-only).
"""

from __future__ import annotations

from nirs4all_cluster.schemas import DatasetRef, JobRequest, PipelineRef


def _req(pipeline=None, pipelines=None, dataset=None, datasets=None) -> JobRequest:
    if pipeline is None and pipelines is None:
        pipeline = PipelineRef(kind="path", path="/p.yaml")
    if dataset is None and datasets is None:
        dataset = DatasetRef(kind="shared_path", path="/d")
    return JobRequest(pipeline=pipeline, pipelines=pipelines, dataset=dataset, datasets=datasets)


def _inline(doc) -> PipelineRef:
    return PipelineRef(kind="inline_json", inline=doc)


def test_atomic_single_pipeline_dataset():
    assert _req().inferred_scheduler_contract().shape == "atomic"


def test_plain_steps_list_is_not_dag():
    # A steps list with no after/deps edges is a linear pipeline, not a DAG.
    ref = _inline({"steps": [{"class": "SNV"}, {"class": "PLS"}]})
    assert _req(pipeline=ref).inferred_scheduler_contract().shape == "atomic"


def test_matrix_without_dag_markers():
    req = _req(
        pipelines=[PipelineRef(kind="path", path="/a.yaml"), PipelineRef(kind="path", path="/b.yaml")],
        datasets=[DatasetRef(kind="shared_path", path="/x"), DatasetRef(kind="shared_path", path="/y")],
    )
    assert req.inferred_scheduler_contract().shape == "pipeline_dataset_matrix"


def test_single_pipeline_over_two_datasets_is_matrix():
    req = _req(datasets=[DatasetRef(kind="shared_path", path="/x"), DatasetRef(kind="shared_path", path="/y")])
    assert req.inferred_scheduler_contract().shape == "pipeline_dataset_matrix"


def test_dagml_key_is_dag_shaped():
    ref = _inline({"dagml": {"nodes": [{"id": "a", "op": "DATASET", "deps": []}]}})
    assert _req(pipeline=ref).inferred_scheduler_contract().shape == "dag_shaped_whole_run"


def test_dag_key_is_dag_shaped():
    ref = _inline({"dag": {"nodes": [{"id": "a"}]}})
    assert _req(pipeline=ref).inferred_scheduler_contract().shape == "dag_shaped_whole_run"


def test_after_edges_in_step_list_are_dag_shaped():
    ref = _inline({"steps": [{"id": "snv", "class": "SNV"}, {"id": "pls", "class": "PLS", "after": ["snv"]}]})
    assert _req(pipeline=ref).inferred_scheduler_contract().shape == "dag_shaped_whole_run"


def test_deps_edges_in_pipeline_list_are_dag_shaped():
    ref = _inline({"pipeline": [{"id": "a"}, {"id": "b", "deps": ["a"]}]})
    assert _req(pipeline=ref).inferred_scheduler_contract().shape == "dag_shaped_whole_run"


def test_dag_beats_matrix_precedence():
    # A matrix that includes one DAG-shaped pipeline is classified as DAG.
    dag = _inline({"dagml": {"nodes": [{"id": "a", "deps": []}]}})
    plain = PipelineRef(kind="path", path="/b.yaml")
    req = _req(
        pipelines=[plain, dag],
        datasets=[DatasetRef(kind="shared_path", path="/x"), DatasetRef(kind="shared_path", path="/y")],
    )
    assert req.inferred_scheduler_contract().shape == "dag_shaped_whole_run"


def test_non_inline_pipeline_is_never_dag_shaped():
    # Shape detection is best-effort and only inspects inline_json payloads.
    ref = PipelineRef(kind="path", path="/dag-on-disk.yaml")
    assert _req(pipeline=ref).inferred_scheduler_contract().shape == "atomic"
