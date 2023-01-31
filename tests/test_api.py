from unittest import TestCase

from fastapi.testclient import TestClient

from ftmstore_fastapi.api import app


class ApiTestCase(TestCase):
    client = TestClient(app)
    maxDiff = None

    def test_api_1(self):
        res = self.client.get("/")
        self.assertEqual(200, res.status_code)

    def test_api_catalog(self):
        res = self.client.get("/catalog")
        self.assertEqual(200, res.status_code)
        self.assertDictEqual(
            res.json(),
            {
                "datasets": [
                    {
                        "name": "ec_meetings",
                        "title": "Ec Meetings",
                        "summary": None,
                        "url": None,
                        "load": False,
                        "entities_url": None,
                        "version": "1",
                        "children": [],
                    },
                    {
                        "name": "eu_authorities",
                        "title": "Eu Authorities",
                        "summary": None,
                        "url": None,
                        "load": False,
                        "entities_url": None,
                        "version": "1",
                        "children": [],
                    },
                ]
            },
        )

    def test_api_dataset_detail(self):
        res = self.client.get("/ec_meetings")
        self.assertEqual(200, res.status_code)
        self.assertDictEqual(
            res.json(),
            {
                "name": "ec_meetings",
                "title": "Ec Meetings",
                "summary": None,
                "url": None,
                "load": False,
                "entities_url": None,
                "version": "1",
                "children": [],
            },
        )
        res = self.client.get("/not_existent")
        self.assertEqual(422, res.status_code)  # validation error

    def test_api_dataset_entities(self):
        res = self.client.get("/ec_meetings/entities")
        self.assertEqual(200, res.status_code)
        res = res.json()
        self.assertEqual(res["total"], 45038)
        self.assertEqual(res["items"], 100)

        res = self.client.get("/ec_meetings/entities?limit=1")
        self.assertEqual(200, res.status_code)
        res = res.json()
        self.assertEqual(res["total"], 45038)
        self.assertEqual(res["items"], 1)
        self.assertSequenceEqual(
            res["entities"],
            [
                {
                    "id": "addr-00177d9455d8e1b6a3f5530ea1e7e81ce1c8333f",
                    "caption": "SIllicon Valley",
                    "schema": "Address",
                    "properties": {"full": ["SIllicon Valley"]},
                    "datasets": ["ec_meetings"],
                    "referents": [],
                    "context": {},
                }
            ],
        )

    def test_api_dataset_entity_detail(self):
        res = self.client.get(
            "/ec_meetings/entities/addr-00177d9455d8e1b6a3f5530ea1e7e81ce1c8333f"
        )
        res = res.json()
        self.assertDictEqual(
            res,
            {
                "id": "addr-00177d9455d8e1b6a3f5530ea1e7e81ce1c8333f",
                "caption": "SIllicon Valley",
                "schema": "Address",
                "properties": {"full": ["SIllicon Valley"]},
                "datasets": ["ec_meetings"],
                "referents": [],
                "context": {},
            },
        )

    def test_api_search(self):
        res = self.client.get("/eu_authorities/search?q=germany&dehydrate=1")
        res = res.json()
        self.assertEqual(res["items"], 1)
        # self.assertDictEqual(
        #     res,
        #     {
        #         "total": 1,
        #         "items": 1,
        #         "query": {
        #             "limit": 100,
        #             "page": 1,
        #             "schema": None,
        #             "order_by": None,
        #             "prop": None,
        #             "value": None,
        #         },
        #         "url": "http://testserver/eu_authorities/search?q=germany&dehydrate=true&limit=100&page=1",
        #         "next_url": None,
        #         "prev_url": None,
        #         "entities": [
        #             {
        #                 "id": "eu-authorities-permanent-representation-of-germany-to-the-eu",
        #                 "caption": "Permanent Representation of Germany to the EU",
        #                 "schema": "PublicBody",
        #                 "properties": {
        #                     "name": ["Permanent Representation of Germany to the EU"],
        #                     "legalForm": ["not_1049", "permanent_representative"],
        #                 },
        #                 "datasets": ["eu_authorities"],
        #                 "referents": [],
        #                 "context": {"referents": [], "datasets": ["eu_authorities"]},
        #             }
        #         ],
        #     },
        # )
