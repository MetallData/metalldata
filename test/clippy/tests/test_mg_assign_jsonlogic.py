# Copyright Lawrence Livermore National Security, LLC and other MetallData
# Project Developers. See the top-level COPYRIGHT file for details.
#
# SPDX-License-Identifier: MIT

# Tests for assign() with a jsonlogic expression as the value.

import re

import pytest
from clippy.backends.fs.execution import NonZeroReturnCodeError  # type: ignore


def warning_count(output: str, msg: str) -> int:
    """Returns the global count reported for warning `msg`, or 0 if absent."""
    m = re.search(re.escape(msg) + r" : (\d+)", output)
    return int(m.group(1)) if m else 0


def assert_not_created(mg, name: str):
    table, _ = name.split(".", 1)
    rows = mg.select_nodes() if table == "node" else mg.select_edges()
    assert all(name not in r for r in rows)


#
# Successful assignment and type inference
#


def test_mg_assign_jsonlogic_int(metallgraph):
    mg = metallgraph
    mg.assign("edge.isum", {"+": [{"var": "edge.graphnum"}, {"var": "edge.randint"}]})
    edges = mg.select_edges()
    assert len(edges) > 0
    for e in edges:
        assert e["edge.isum"] == e["edge.graphnum"] + e["edge.randint"]
        assert isinstance(e["edge.isum"], int)


def test_mg_assign_jsonlogic_double(metallgraph):
    mg = metallgraph
    # edge.weight is a double series, so int + double infers a double series
    mg.assign("edge.wsum", {"+": [{"var": "edge.weight"}, {"var": "edge.randint"}]})
    for e in mg.select_edges():
        assert e["edge.wsum"] == pytest.approx(e["edge.weight"] + e["edge.randint"])
        assert isinstance(e["edge.wsum"], float)


def test_mg_assign_jsonlogic_double_literal(metallgraph):
    mg = metallgraph
    mg.assign("edge.half", {"*": [{"var": "edge.randint"}, 0.5]})
    for e in mg.select_edges():
        assert e["edge.half"] == pytest.approx(e["edge.randint"] * 0.5)


def test_mg_assign_jsonlogic_string(metallgraph):
    mg = metallgraph
    mg.assign("edge.label", {"cat": [{"var": "edge.color"}, "-", {"var": "edge.name"}]})
    edges = mg.select_edges()
    assert any("edge.label" in e for e in edges)
    for e in edges:
        # some edges have no color or name; those rows are left unset
        if "edge.color" in e and "edge.name" in e:
            assert e["edge.label"] == f"{e['edge.color']}-{e['edge.name']}"
        else:
            assert "edge.label" not in e


def test_mg_assign_jsonlogic_bool_from_expression(metallgraph):
    mg = metallgraph
    # clippy expression objects serialize as {"rule": ...}
    mg.assign("edge.big", mg.edge.randint > 50)
    for e in mg.select_edges():
        assert e["edge.big"] == (e["edge.randint"] > 50)


def test_mg_assign_jsonlogic_where(metallgraph):
    mg = metallgraph
    mg.assign("edge.w2", {"*": [{"var": "edge.weight"}, 2]}, where=mg.edge.graphnum == 3)
    edges = mg.select_edges()
    assert any("edge.w2" in e for e in edges)
    for e in edges:
        if e["edge.graphnum"] == 3:
            assert e["edge.w2"] == e["edge.weight"] * 2
        else:
            assert "edge.w2" not in e


def test_mg_assign_jsonlogic_node(metallgraph):
    mg = metallgraph
    # node.gnum is only set on some nodes (see conftest); the others stay unset.
    mg.assign("node.gnum1", {"+": [{"var": "node.gnum"}, 1]})
    nodes = mg.select_nodes()
    assert any("node.gnum" in n for n in nodes)
    assert any("node.gnum" not in n for n in nodes)
    for n in nodes:
        if "node.gnum" in n:
            assert n["node.gnum1"] == n["node.gnum"] + 1
        else:
            assert "node.gnum1" not in n


def test_mg_assign_constant_still_works(metallgraph):
    mg = metallgraph
    mg.assign("edge.const_str", "blue", where=mg.edge.graphnum == 3)
    mg.assign("node.const_int", 7)
    for e in mg.select_edges():
        if e["edge.graphnum"] == 3:
            assert e["edge.const_str"] == "blue"
        else:
            assert "edge.const_str" not in e
    assert all(n["node.const_int"] == 7 for n in mg.select_nodes())


#
# Errors: the call fails with a specific message and no series is created
#


def test_mg_assign_jsonlogic_error_series_exists(metallgraph):
    with pytest.raises(NonZeroReturnCodeError, match="already exists"):
        metallgraph.assign("edge.weight", {"+": [{"var": "edge.weight"}, 1]})


@pytest.mark.parametrize("name", ["nodot", "foo.bar"])
def test_mg_assign_jsonlogic_error_bad_target(metallgraph, name):
    with pytest.raises(NonZeroReturnCodeError, match="unknown series name"):
        metallgraph.assign(name, {"+": [{"var": "edge.randint"}, 1]})


def test_mg_assign_jsonlogic_error_cross_table(metallgraph):
    mg = metallgraph
    with pytest.raises(NonZeroReturnCodeError, match="is not a node series"):
        mg.assign("node.bad", {"+": [{"var": "edge.randint"}, 1]})
    assert_not_created(mg, "node.bad")
    with pytest.raises(NonZeroReturnCodeError, match="is not an edge series"):
        mg.assign("edge.bad", {"+": [{"var": "node.gnum"}, 1]})
    assert_not_created(mg, "edge.bad")


def test_mg_assign_jsonlogic_error_unknown_variable(metallgraph):
    mg = metallgraph
    with pytest.raises(NonZeroReturnCodeError, match="edge.does_not_exist not found"):
        mg.assign("edge.bad", {"var": "edge.does_not_exist"})
    assert_not_created(mg, "edge.bad")


def test_mg_assign_jsonlogic_error_invalid_operator(metallgraph):
    mg = metallgraph
    with pytest.raises(NonZeroReturnCodeError, match="invalid jsonlogic expression"):
        mg.assign("edge.bad", {"no_such_operator": [{"var": "edge.randint"}, 1]})
    assert_not_created(mg, "edge.bad")


def test_mg_assign_jsonlogic_error_computed_variable(metallgraph):
    mg = metallgraph
    with pytest.raises(NonZeroReturnCodeError, match="computed variable names"):
        mg.assign("edge.bad", {"var": {"cat": ["edge.", "color"]}})
    assert_not_created(mg, "edge.bad")


def test_mg_assign_jsonlogic_error_where_matches_nothing(metallgraph):
    mg = metallgraph
    with pytest.raises(NonZeroReturnCodeError, match="produced no values"):
        mg.assign(
            "edge.bad", {"+": [{"var": "edge.randint"}, 1]}, where=mg.edge.graphnum == 999
        )
    assert_not_created(mg, "edge.bad")


def test_mg_assign_jsonlogic_error_where_unknown_series(metallgraph):
    mg = metallgraph
    where = {"rule": {"==": [{"var": "edge.does_not_exist"}, 1]}}
    with pytest.raises(NonZeroReturnCodeError, match="produced no values"):
        mg.assign("edge.bad", {"+": [{"var": "edge.randint"}, 1]}, where=where)
    assert_not_created(mg, "edge.bad")


def test_mg_assign_jsonlogic_error_every_row_raises(metallgraph):
    # jsonlogic throws on "string" + 1. Every row fails, so no type can be
    # inferred. (This used to deadlock the ranks.)
    mg = metallgraph
    with pytest.raises(NonZeroReturnCodeError, match="produced no values"):
        mg.assign("edge.bad", {"+": [{"var": "edge.color"}, 1]})
    assert_not_created(mg, "edge.bad")


def test_mg_assign_jsonlogic_error_every_row_array(metallgraph):
    mg = metallgraph
    with pytest.raises(NonZeroReturnCodeError, match="produced no values"):
        mg.assign("edge.bad", {"merge": [[{"var": "edge.randint"}], [2]]})
    assert_not_created(mg, "edge.bad")


#
# Warnings: the call succeeds, skipped rows are left unset and counted
#


def test_mg_assign_jsonlogic_warn_missing_input(metallgraph, capsys):
    mg = metallgraph
    mg.assign("node.gnum1", {"+": [{"var": "node.gnum"}, 1]})
    out = capsys.readouterr().out
    missing = sum(1 for n in mg.select_nodes() if "node.gnum" not in n)
    assert missing > 0
    assert warning_count(out, "row skipped: an input variable is missing") == missing


def test_mg_assign_jsonlogic_warn_null_result(metallgraph, capsys):
    mg = metallgraph
    mg.assign("edge.maybe", {"if": [{"var": "edge.relevant"}, 1, None]})
    out = capsys.readouterr().out
    edges = mg.select_edges()
    nulls = sum(1 for e in edges if "edge.relevant" in e and not e["edge.relevant"])
    assert nulls > 0
    assert warning_count(out, "row skipped: expression returned null") == nulls
    for e in edges:
        if e.get("edge.relevant"):
            assert e["edge.maybe"] == 1
        else:
            assert "edge.maybe" not in e


def test_mg_assign_jsonlogic_warn_some_rows_raise(metallgraph, capsys):
    mg = metallgraph
    rule = {"if": [{"var": "edge.relevant"}, 1, {"+": [{"var": "edge.color"}, 1]}]}
    mg.assign("edge.partial", rule)
    out = capsys.readouterr().out
    edges = mg.select_edges()
    complete = [e for e in edges if "edge.relevant" in e and "edge.color" in e]
    raised = sum(1 for e in complete if not e["edge.relevant"])
    missing = len(edges) - len(complete)
    assert raised > 0
    assert warning_count(out, "row skipped: expression raised an error") == raised
    assert warning_count(out, "row skipped: an input variable is missing") == missing
    for e in edges:
        if e in complete and e["edge.relevant"]:
            assert e["edge.partial"] == 1
        else:
            assert "edge.partial" not in e


def test_mg_assign_jsonlogic_mixed_string_and_number(metallgraph, capsys):
    # Each rank picks its type from its first value. If ranks disagree
    # (string vs number) the call fails; if they agree, the rows of the other
    # type are skipped with a warning. Which happens depends on partitioning.
    mg = metallgraph
    rule = {"if": [{"var": "edge.relevant"}, "yes", 1]}
    try:
        mg.assign("edge.mixed", rule)
    except NonZeroReturnCodeError as ex:
        assert "both string and int64" in str(ex)
        assert_not_created(mg, "edge.mixed")
        return
    out = capsys.readouterr().out
    skipped = warning_count(
        out, "row skipped: result could not be stored in a string series"
    ) + warning_count(out, "row skipped: result could not be stored in an int64 series")
    assert skipped > 0
