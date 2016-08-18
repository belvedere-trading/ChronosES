from __future__ import with_statement, print_function
from alembic import context
from sqlalchemy import engine_from_config, pool
from logging.config import fileConfig
import imp
import os
import sys
import traceback
sys.path.append('.') # Makes aggregates available

config = context.config
fileConfig(config.config_file_name)

def import_logic(aggregate_name):
    impfile = None
    try:
        impfile, pathname, desc = imp.find_module(aggregate_name)
        return imp.load_module(aggregate_name, impfile, pathname, desc)
    except Exception:
        traceback.print_exc()
        print('Error: failed to load aggregate module {0}'.format(aggregate_name), file=sys.stderr)
        sys.exit(-3)
    finally:
        if impfile:
            impfile.close()

def run_migrations(aggregate_name):
    aggregate_module = import_logic(aggregate_name)
    if not hasattr(aggregate_module.AggregateClass, 'Index'):
        return
    aggregate_class = aggregate_module.AggregateClass
    aggregate_class.Index.CreateTagModel()
    aggregate_class.Index.CreateCheckpointModel()

    connectable = engine_from_config(
        config.get_section(config.config_ini_section),
        prefix='sqlalchemy.',
        url='sqlite:////var/lib/chronos/index.alembic/db/{0}.sqlite'.format(aggregate_name),
        poolclass=pool.NullPool)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=aggregate_class.Index.metadata,
            upgrade_token='{0}_upgrades'.format(aggregate_name),
            downgrade_token='{0}_downgrades'.format(aggregate_name),
            compare_type=True,
            render_as_batch=True)

        with context.begin_transaction():
            context.run_migrations(engine_name=aggregate_name)

if context.is_offline_mode():
    print('Error: cannot run Chronos migrations in offline mode', file=sys.stderr)
    sys.exit(-1)
else:
    aggregate_name = os.getenv('AGGREGATE_NAME', None)
    if aggregate_name is None:
        print('Error: AGGREGATE_NAME environment variable not set', file=sys.stderr)
        sys.exit(-2)
    run_migrations(aggregate_name)
