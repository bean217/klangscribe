import dagster as dg


@dg.op
def test_print(context: dg.OpExecutionContext):
    context.log.info("Print Test")


@dg.job
def sanity():
    test_print()
