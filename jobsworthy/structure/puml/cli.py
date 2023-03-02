import click

from . import model_maker, puml_maker


@click.group()
def cli():
    pass


@click.option('--puml-model', '-p', required=True, help="Location of the PlantUML Model")
@click.option('--schema-location',
              '-s',
              required=False,
              help="Location to write the schema.  When not provided, schema and vocab are not generated")
@click.option('--vocab-location',
              '-v',
              required=False,
              help="Location to write the vocab.  When not provided, schema and vocab are not generated")
@click.option('--repo-location',
              '-r',
              required=False,
              help="Location to write the Repo stub.  When not provided, the repo is not generated")
@click.command()
def generate_schema(puml_model, schema_location, vocab_location, repo_location):
    """
    Generates a schema, vocab and repo compatible with the Structure module from a PUML diagram.
    """
    model_maker.make(puml_model=puml_model,
                     schema_location=schema_location,
                     vocab_location=vocab_location,
                     repo_location=repo_location)

    pass

@click.option('--print-schema', '-s', required=True, help="Location of the Print Schema Output")
@click.option('--puml-out', '-p', required=True, help="Location to write the PUML Model")
@click.option('--model', '-m', required=True, help="Name of the PUML Model")
@click.command()
def puml_from_print_schema(print_schema, puml_out, model):
    """
    Create a puml class model from the output of a Spark dataframe.printSchema() command.
    """
    puml_maker(spark_schema_location=print_schema, model_name=model, output_to=puml_out)



cli.add_command(generate_schema)
cli.add_command(puml_from_print_schema)
