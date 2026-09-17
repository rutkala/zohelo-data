"""Pure selection bookkeeping for BDL Web downloads (never parses source data)."""
from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from math import prod
import re


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def digest(value):
    return sha256(canonical(value)).hexdigest()


def values(items):
    answer = [item["value"] for item in items]
    if not answer or any(not isinstance(x, str) or not x for x in answer) or len(set(answer)) != len(answer):
        raise ValueError("Missing, empty or duplicate provider selection identifiers")
    return answer


def task(kind, dimensions=None, layout=None, territories=None):
    scope = deepcopy({"kind": kind, "dimensions": dimensions or {}, "layout": layout, "territories": territories})
    return {"id": digest(scope), "scope": scope, "status": "pending", "attempts": 0}


def new_plan(subgroup, url):
    if not re.fullmatch(r"P\d+", subgroup):
        raise ValueError("Invalid subgroup")
    root = task("dimensions")
    return {"format_version": 1, "source_id": "gus_bdl", "transport": "web_ui",
            "record_type": "selection_partition_plan", "subgroup_id": subgroup, "url": url,
            "root": root["id"], "nodes": {root["id"]: root}, "dimension_inventory": []}


def _expand(plan, node, children, rule, axis=None, inventory=None):
    if node["status"] not in {"pending", "retry"} or not children:
        raise ValueError("Cannot expand this selection")
    if any(c["id"] in plan["nodes"] for c in children) or len({c["id"] for c in children}) != len(children):
        raise ValueError("Selection split duplicated work")
    node.update(status="expanded", children=[c["id"] for c in children], rule=rule, axis=axis)
    if inventory is not None:
        node["inventory"] = inventory
    plan["nodes"].update((c["id"], c) for c in children)
    validate(plan)


def dimension_axis(plan, scope):
    candidates = [k for k, v in scope["dimensions"].items() if len(v) > 1]
    years = {d["id"] for d in plan["dimension_inventory"] if d.get("year_axis")}
    return next((k for k in candidates if k in years), None) or max(
        candidates, key=lambda k: len(scope["dimensions"][k]), default=None)


def split(plan, node):
    scope = node["scope"]
    axis = "territories" if len(scope.get("territories") or []) > 1 else dimension_axis(plan, scope)
    if axis is None:
        node["status"] = "blocked"
        return False
    old = scope["territories"] if axis == "territories" else scope["dimensions"][axis]
    midpoint = len(old) // 2
    children = []
    for portion in (old[:midpoint], old[midpoint:]):
        part = deepcopy(scope)
        if axis == "territories":
            part["territories"] = portion
        else:
            part["dimensions"][axis] = portion
        children.append(task(**part))
    _expand(plan, node, children, "partition", axis=axis)
    return True


def accept_dimensions(plan, node, inventory):
    if node["scope"]["kind"] != "dimensions" or not inventory:
        raise ValueError("Invalid dimension inventory response")
    keys = [d["id"] for d in inventory]
    if len(set(keys)) != len(keys):
        raise ValueError("Duplicate dimension controls")
    selection = {d["id"]: values(d["options"]) for d in inventory}
    plan["dimension_inventory"] = inventory
    _expand(plan, node, [task("layouts", selection)], "dimensions", inventory=selection)


def accept_layouts(plan, node, layouts):
    if node["scope"]["kind"] != "layouts":
        raise ValueError("Unexpected layout response")
    values(layouts)
    _expand(plan, node, [task("territories", node["scope"]["dimensions"], layout) for layout in layouts],
            "layouts", inventory=layouts)


def accept_territories(plan, node, items, advertised_count, initial_size=25, max_size=250, cell_budget=200000):
    if node["scope"]["kind"] != "territories":
        raise ValueError("Unexpected territory response")
    identifiers = values(items)
    if type(advertised_count) is not int or len(identifiers) != advertised_count:
        raise ValueError("Territorial list exhaustion was not established")
    selection = node["scope"]["dimensions"]
    combinations = prod(map(len, selection.values()))
    size = max(1, min(max_size, cell_budget // max(1, combinations)))
    # Retain the initial proof transfers. They are genuine partitions, not a sample completion.
    preferred = next((i["value"] for i in items if plan["subgroup_id"] == "P1313" and i["label"] == "Bolesławiec (1)"), identifiers[0])
    ordered = [preferred] + [v for v in identifiers if v != preferred]
    chunks = [ordered[:1]]
    tail = ordered[1:]
    if tail:
        first = min(initial_size, size)
        chunks.append(tail[:first])
        tail = tail[first:]
    chunks.extend(tail[i:i + size] for i in range(0, len(tail), size))
    _expand(plan, node, [task("download", selection, node["scope"]["layout"], chunk) for chunk in chunks],
            "territories", inventory=items)


def next_task(plan, attempted):
    for node in list(plan["nodes"].values()):
        if node["status"] not in {"pending", "retry"} or node["id"] in attempted:
            continue
        if node["scope"]["kind"] == "layouts" and prod(map(len, node["scope"]["dimensions"].values())) > 3500:
            split(plan, node)
            return next_task(plan, attempted)
        return node
    return None


def failed(plan, node, error, failure_class):
    node["attempts"] += 1
    node["last_error"] = str(error)[:1200]
    node["failure_class"] = failure_class
    if failure_class not in {"provider_error", "provider_timeout"}:
        node["status"] = "blocked"
        return "stop"
    node["status"] = "retry"
    if node["attempts"] >= 2:
        if node["scope"]["kind"] == "download":
            return "split" if split(plan, node) else "blocked"
        node["status"] = "blocked"
        return "blocked"
    return "retry"


def accept_download(plan, node, receipt):
    if node["scope"]["kind"] != "download" or node["status"] not in {"pending", "retry"}:
        raise ValueError("Not a pending native download")
    obj = receipt.get("archive_object", {})
    if (receipt.get("subgroup_id") != plan["subgroup_id"] or receipt.get("selection_id") != node["id"]
            or receipt.get("selection") != node["scope"] or receipt.get("landing_scope") != "native_bytes_only"
            or not obj.get("id") or not re.fullmatch(r"[a-f0-9]{64}", obj.get("sha256", ""))
            or type(obj.get("size")) is not int or obj["size"] <= 0):
        raise ValueError("Native receipt is missing or covers a different selection")
    node.update(status="landed", receipt=receipt)
    validate(plan)


def _disjoint_union(parts, expected):
    flat = [value for part in parts for value in part]
    if not flat or len(flat) != len(set(flat)) or set(flat) != set(expected):
        raise ValueError("Selection partition has gaps or overlaps")


def validate(plan):
    """Prove each expansion preserves exactly its parent's UI selection scope."""
    if plan.get("record_type") != "selection_partition_plan":
        raise ValueError("Unexpected partition record")
    visited = set()
    def walk(key):
        if key in visited:
            raise ValueError("Cyclic or shared partition child")
        visited.add(key)
        node = plan["nodes"][key]
        s = node["scope"]
        if key != node["id"] or key != digest(s):
            raise ValueError("Partition identity changed")
        if node["status"] == "expanded":
            children = [plan["nodes"][c] for c in node["children"]]
            scopes = [c["scope"] for c in children]
            rule = node["rule"]
            if rule == "partition":
                axis = node["axis"]
                expected = s["territories"] if axis == "territories" else s["dimensions"][axis]
                portions = []
                for c in scopes:
                    copy = deepcopy(c)
                    portions.append(copy["territories"] if axis == "territories" else copy["dimensions"][axis])
                    if axis == "territories":
                        copy["territories"] = expected
                    else:
                        copy["dimensions"][axis] = expected
                    if copy != s:
                        raise ValueError("Split changed an unrelated selection axis")
                _disjoint_union(portions, expected)
            elif rule == "dimensions":
                if scopes != [task("layouts", node["inventory"])["scope"]]:
                    raise ValueError("Dimension inventory is not fully planned")
            elif rule == "layouts":
                expected = [task("territories", s["dimensions"], x)["scope"] for x in node["inventory"]]
                if scopes != expected:
                    raise ValueError("Layout inventory is not fully planned")
            elif rule == "territories":
                for c in scopes:
                    if c["kind"] != "download" or c["dimensions"] != s["dimensions"] or c["layout"] != s["layout"]:
                        raise ValueError("Territorial planning changed dimensions or layout")
                _disjoint_union([c["territories"] for c in scopes], values(node["inventory"]))
            else:
                raise ValueError("Unknown partition expansion")
            for c in node["children"]:
                walk(c)
        elif node["status"] == "landed":
            receipt = node.get("receipt", {})
            if s["kind"] != "download" or receipt.get("selection_id") != key or receipt.get("selection") != s:
                raise ValueError("Unbound completion receipt")
        elif node["status"] not in {"pending", "retry", "blocked"}:
            raise ValueError("Unknown selection status")
    walk(plan["root"])
    if visited != set(plan["nodes"]):
        raise ValueError("Unreachable work in partition plan")


def summary(plan):
    validate(plan)
    leaves = [n for n in plan["nodes"].values() if n["status"] != "expanded"]
    landed = [n for n in leaves if n["status"] == "landed"]
    return {"complete": bool(leaves) and len(landed) == len(leaves), "files": len(landed),
            "bytes": sum(n["receipt"]["archive_object"]["size"] for n in landed),
            "outstanding_selections": len(leaves) - len(landed),
            "blocked_selections": sum(n["status"] == "blocked" for n in leaves),
            "completion_scope": "all_planned_web_selections", "content_validation": "not_performed"}
