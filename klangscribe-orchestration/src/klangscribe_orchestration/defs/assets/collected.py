import dagster as dg


@dg.asset
def collected(context: dg.AssetExecutionContext) -> dg.MaterializeResult: ...
