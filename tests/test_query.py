from unittest import TestCase

from fastapi import HTTPException

from ftmstore_fastapi.query import (
    AggregationParams,
    AggregationQuery,
    Query,
    SearchQuery,
)


class QueryTestCase(TestCase):
    maxDiff = None
    table = "ftm_test"

    def test_query(self):
        q = Query(self.table)
        self.assertEqual(str(q), "SELECT t.id, t.schema, t.entity FROM ftm_test t")

        q = Query("ftm_test_other")
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity FROM ftm_test_other t",
        )

        q = Query(self.table).where(country="de")
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.country') AS country FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(country) WHERE value = ?))",
        )

        q = Query(self.table).where(date=2019, country="de")
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.country') AS country, json_extract(t.entity, '$.properties.date') AS date FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(country) WHERE value = ?)) AND (EXISTS (SELECT 1 FROM json_each(date) WHERE value = ?))",
        )
        self.assertSequenceEqual(["de", 2019], [v for v in q.parameters])

        q = Query(self.table).order_by("id")
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity FROM ftm_test t ORDER BY id ASC",
        )

        q = Query(self.table).order_by("id", "entity", ascending=False)
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity FROM ftm_test t ORDER BY id, entity DESC",
        )

    def test_query_extra_data(self):
        q = (
            Query(self.table)
            .where(**{"context.scope": "foo"})
            .order_by("context.scope")
        )
        self.assertEqual(
            q.select_part,
            "t.id, t.schema, t.entity, json_extract(t.entity, '$.scope') AS context_scope",
        )
        self.assertEqual(q.where_part, "WHERE (context_scope = ?)")
        self.assertSequenceEqual(["foo"], [v for v in q.parameters])
        self.assertEqual(q.order_part, "ORDER BY context_scope ASC")
        q = Query(self.table).where(**{"context.listish[]": "foo"})
        self.assertEqual(
            q.where_part,
            "WHERE (EXISTS (SELECT 1 FROM json_each(context_listish) WHERE value = ?))",
        )

    def test_query_where_operators(self):
        base = Query(self.table)

        q = base.where(schema="Person")
        self.assertEqual(str(q), base.to_str() + " WHERE (t.schema = ?)")
        self.assertSequenceEqual(["Person"], [v for v in q.parameters])

        q = Query(self.table).where(schema="Payment", amount__gt=0)
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.amount') AS amount FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(amount) WHERE CAST(value AS NUMERIC) > ?)) AND (t.schema = ?)",
        )
        self.assertSequenceEqual([0, "Payment"], [v for v in q.parameters])

        q = Query(self.table).where(schema="Payment", amount__gte=10)
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.amount') AS amount FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(amount) WHERE CAST(value AS NUMERIC) >= ?)) AND (t.schema = ?)",
        )
        self.assertSequenceEqual([10, "Payment"], [v for v in q.parameters])

        q = Query(self.table).where(schema="Payment", amount__lt=10)
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.amount') AS amount FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(amount) WHERE CAST(value AS NUMERIC) < ?)) AND (t.schema = ?)",
        )
        self.assertSequenceEqual([10, "Payment"], [v for v in q.parameters])

        q = Query(self.table).where(schema="Payment", amount__lte=10)
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.amount') AS amount FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(amount) WHERE CAST(value AS NUMERIC) <= ?)) AND (t.schema = ?)",
        )
        self.assertSequenceEqual([10, "Payment"], [v for v in q.parameters])

        q = Query(self.table).where(name__like="nestle")
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.name') AS name FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(name) WHERE value LIKE ?))",
        )
        # FIXME sqlite COLLATE NOCASE
        self.assertEqual(str(q), str(q.where(name__ilike="nestle")))

        q = Query(self.table).where(name__in=("alice", "lisa"))
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.name') AS name FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(name) WHERE value IN (?, ?)))",
        )
        self.assertSequenceEqual(["alice", "lisa"], [v for v in q.parameters])

    def test_query_slice(self):
        base = Query(self.table)
        q = base[:100]
        self.assertEqual(str(q), f"{base} LIMIT 100 OFFSET 0")

        q = base[100:200]
        self.assertEqual(str(q), f"{base} LIMIT 100 OFFSET 100")

        q = base[100:]
        self.assertEqual(str(q), f"{base} LIMIT -1 OFFSET 100")

        q = base[17]
        self.assertEqual(str(q), f"{base} LIMIT 1 OFFSET 17")

    def test_query_order_by(self):
        base = Query(self.table)
        self.assertIsNone(base.order_part)
        self.assertEqual(base.order_by("id").order_part, "ORDER BY id ASC")
        self.assertEqual(
            base.order_by("id", ascending=False).order_part, "ORDER BY id DESC"
        )
        self.assertEqual(base.order_by("name").order_part, "ORDER BY name ASC")
        self.assertEqual(
            base.order_by("name").select_part,
            "t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.name') AS name",
        )

    def test_query_correct_chain(self):
        q = Query(self.table).where(name="bar").order_by("date")[:10].where(amount=1)
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.amount') AS amount, json_extract(t.entity, '$.properties.date') AS date, json_extract(t.entity, '$.properties.name') AS name FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(amount) WHERE CAST(value AS NUMERIC) = ?)) AND (EXISTS (SELECT 1 FROM json_each(name) WHERE value = ?)) ORDER BY date ASC LIMIT 10 OFFSET 0",
        )
        # order by should be overwritten!
        q = Query(self.table).order_by("a").order_by("b")
        self.assertEqual(q.order_part, "ORDER BY b ASC")

    def test_query_lookup_null(self):
        q = Query(self.table).where(name__null=True)
        self.assertEqual(
            q.where_part,
            "WHERE (EXISTS (SELECT 1 FROM json_each(name) WHERE value IS NULL))",
        )
        q = Query(self.table).where(name__null=False)
        self.assertEqual(
            q.where_part,
            "WHERE (EXISTS (SELECT 1 FROM json_each(name) WHERE value IS NOT NULL))",
        )
        self.assertSequenceEqual([], [x for x in q.parameters])

    def test_query_invalid(self):
        with self.assertRaisesRegex(HTTPException, "400"):
            q = Query(self.table)[-1]
            str(q)

        with self.assertRaisesRegex(HTTPException, "400"):
            q = Query(self.table)[100:50]
            str(q)

        with self.assertRaisesRegex(HTTPException, "400"):
            q = Query(self.table)[100:50:2]
            str(q)

        with self.assertRaisesRegex(HTTPException, "400"):
            q = Query(self.table).where(name__invalid_op=0)
            str(q)

        with self.assertRaisesRegex(HTTPException, "400"):
            q = Query(self.table).where(name__invalid__op=0)
            str(q)

        # invalid ftm_columnstore_test props
        with self.assertRaisesRegex(HTTPException, "400"):
            q = Query(self.table).where(invalid_prop=0)
            str(q)

        with self.assertRaisesRegex(HTTPException, "400"):
            q = Query(self.table).where(invalid_prop__like=0)
            str(q)

    def test_query_search(self):
        q = SearchQuery(self.table, term="alice")
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity FROM ftm_test__fts s LEFT JOIN ftm_test t ON s.id = t.id WHERE s.text MATCH ? ORDER BY s.rank",
        )
        self.assertSequenceEqual(["alice"], tuple(q.parameters))
        q = q.where(schema="Person")
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity FROM ftm_test__fts s LEFT JOIN ftm_test t ON s.id = t.id WHERE (t.schema = ?) AND s.text MATCH ? ORDER BY s.rank",
        )
        self.assertSequenceEqual(["Person", "alice"], tuple(q.parameters))
        q = q.where(keywords="foo")
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.keywords') AS keywords FROM ftm_test__fts s LEFT JOIN ftm_test t ON s.id = t.id WHERE (EXISTS (SELECT 1 FROM json_each(keywords) WHERE value = ?)) AND (t.schema = ?) AND s.text MATCH ? ORDER BY s.rank",
        )

        # ordering overwrites rank ordering
        q = q.order_by("name")
        self.assertEqual(q.order_part, "ORDER BY name ASC")

    def test_query_type_cast(self):
        q = Query(self.table).where(amount__gt=10).order_by("amount")
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.amount') AS amount FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(amount) WHERE CAST(value AS NUMERIC) > ?)) ORDER BY CAST(json_extract(amount, '$[0]') AS NUMERIC) ASC",
        )

    def test_query_aggregations(self):
        agg = AggregationParams(aggSum=["amount"])
        q = (
            AggregationQuery(self.table, aggregations=agg)
            .where(amount__gt=10)
            .order_by("amount")
        )
        self.assertEqual(
            str(q),
            "SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.amount') AS amount FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(amount) WHERE CAST(value AS NUMERIC) > ?)) ORDER BY CAST(json_extract(amount, '$[0]') AS NUMERIC) ASC",
        )
        for field, func, q in q.get_agg_queries():
            self.assertEqual(field, "amount")
            self.assertEqual(func, "sum")
            self.assertEqual(
                q,
                "SELECT SUM(value) AS aggSum FROM (SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.amount') AS amount FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(amount) WHERE CAST(value AS NUMERIC) > ?)) ORDER BY CAST(json_extract(amount, '$[0]') AS NUMERIC) ASC) t, json_each(json_extract(t.entity, '$.properties.amount'))",
            )

    def test_query_schemata_groups(self):
        q = Query(self.table)
        self.assertEqual(
            q.schemata,
            "SELECT schema, COUNT(*) FROM (SELECT t.id, t.schema, t.entity FROM ftm_test t) GROUP BY schema",
        )
        self.assertEqual(
            q.schemata, f"SELECT schema, COUNT(*) FROM ({q}) GROUP BY schema"
        )
        q = Query(self.table).where(country="fr")
        self.assertEqual(
            q.schemata, f"SELECT schema, COUNT(*) FROM ({q}) GROUP BY schema"
        )
        self.assertEqual(
            q.schemata,
            "SELECT schema, COUNT(*) FROM (SELECT t.id, t.schema, t.entity, json_extract(t.entity, '$.properties.country') AS country FROM ftm_test t WHERE (EXISTS (SELECT 1 FROM json_each(country) WHERE value = ?))) GROUP BY schema",
        )
