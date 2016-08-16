##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from edgedb.lang.schema import _testbase as tb


class TestEdgeSchemaLoader(tb.LoaderTest):
    def test_eschema_loader_constraint_01(self):
        """
constraint must_be_even:
    expr := subject % 2 == 0

concept ConstraintTest:
    required link even_value to int:
        constraint must_be_even

% OK %

CREATE MODULE test
CREATE CONSTRAINT {test::must_be_even} {
    SET expr := (((subject % 2) = 0))
}
CREATE LINK {test::even_value} INHERITING {std::link} {
    SET mapping := '11'
    SET readonly := False
}
CREATE CONCEPT {test::ConstraintTest} INHERITING {std::Object} {
    SET is_virtual := False
    CREATE LINK {test::even_value} TO {std::int} {
        SET mapping := '11'
        SET readonly := False
        CREATE CONSTRAINT {test::must_be_even} {
            SET expr := (((subject % 2) = 0))
            SET finalexpr := (((subject % 2) = 0))
            SET localfinalexpr := (((subject % 2) = 0))
        }
    }
}
"""
