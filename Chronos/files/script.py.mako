<%!
import os
import re

%>"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

def upgrade(engine_name):
    upgrade_name = 'upgrade_{0}'.format(engine_name)
    upgrade_func = globals().get(upgrade_name, None)
    if upgrade_func is None:
       return
    upgrade_func()

def downgrade(engine_name):
    downgrade_name = 'downgrade_{0}'.format(engine_name)
    downgrade_func = globals().get(downgrade_name, None)
    if downgrade_func is None:
       return
    downgrade_func()

<%
    aggregate_name = os.getenv('AGGREGATE_NAME', None)
%>

def upgrade_${aggregate_name}():
    ${context.get("%s_upgrades" % aggregate_name, "pass")}


def downgrade_${aggregate_name}():
    ${context.get("%s_downgrades" % aggregate_name, "pass")}
