from typing import Any

from workflows.registry import WorkflowSpec


def build_node_info_list(spec: WorkflowSpec, values: dict[str, Any]) -> list[dict[str, str]]:
    merged = dict(spec.fixed_values)
    for key, value in values.items():
        if value is not None and value != "":
            merged[key] = value

    node_info_list: list[dict[str, str]] = []
    for key in spec.node_order:
        if key not in merged:
            continue
        node_id, field_name = spec.nodes[key]
        node_info_list.append(
            {
                "nodeId": node_id,
                "fieldName": field_name,
                "fieldValue": str(merged[key]),
            }
        )
    return node_info_list

