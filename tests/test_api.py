from fastapi.testclient import TestClient

from ftmstore_fastapi.api import app

client = TestClient(app)


def test_api_1():
    res = client.get("/")
    assert res.status_code == 200


def test_api_catalog():
    res = client.get("/catalog")
    assert res.status_code == 200
    data = res.json()
    assert len(data["datasets"]) == 3


def test_api_dataset_detail():
    res = client.get("/catalog/ec_meetings")
    assert res.status_code == 200
    data = res.json()
    assert data["name"] == "ec_meetings"
    assert data["entities_url"] == "http://testserver/entities?dataset=ec_meetings"
    assert (
        data["title"] == "European Commission - Meetings with interest representatives"
    )
    assert data["coverage"]["countries"] == [
        {"code": "eu", "count": 103, "label": "eu"}
    ]

    res = client.get("/not_existent")
    assert res.status_code == 404

    res = client.get("/catalog/not_existent")
    assert res.status_code == 422  # validation error


def test_api_entities():
    res = client.get("/entities")
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 49822
    assert data["items"] == 100

    res = client.get("/entities?dataset=gdho")
    assert res.status_code == 200
    data = res.json()
    combined = data["total"]
    res = client.get("/entities?dataset=eu_authorities")
    assert res.status_code == 200
    data = res.json()
    combined += data["total"]

    res = client.get("/entities?dataset=gdho&dataset=eu_authorities")
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == combined
    assert data["items"] == 100


def test_api_dataset_entities():
    res = client.get("/entities?dataset=ec_meetings")
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 45038
    assert data["items"] == 100

    res = client.get("/entities?dataset=ec_meetings&limit=1")
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 45038
    assert data["items"] == 1
    assert len(data["entities"]) == 1

    res = client.get(
        "/entities?dataset=eu_authorities&jurisdiction=eu&order_by=-name&dehydrate=true"
    )
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 151
    assert data["entities"][0]["id"] == "eu-authorities-cdt"
    assert "jurisdiction" not in data["entities"][0]["properties"]

    res = client.get(
        "/entities?dataset=eu_authorities&jurisdiction=eu&order_by=-name&featured=true"
    )
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 151
    assert data["entities"][0]["id"] == "eu-authorities-cdt"
    assert "jurisdiction" not in data["entities"][0]["properties"]

    res = client.get(
        "/entities?dataset=eu_authorities&jurisdiction__not=eu&order_by=-name"
    )
    assert res.status_code == 200
    data = res.json()
    assert data["total"] == 0


def test_api_dataset_entity_detail():
    res = client.get("/entities/addr-00177d9455d8e1b6a3f5530ea1e7e81ce1c8333f")
    data = res.json()
    assert data == {
        "id": "addr-00177d9455d8e1b6a3f5530ea1e7e81ce1c8333f",
        "caption": "SIllicon Valley",
        "schema": "Address",
        "properties": {"full": ["SIllicon Valley"]},
        "datasets": ["ec_meetings"],
        "referents": [],
    }


# def test_api_search(self):
#     res = self.client.get("/eu_authorities/entities?q=germany&dehydrate=1")
#     res = res.json()
#     self.assertEqual(res["items"], 1)
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


def test_api_reversed():
    res = client.get(
        "/entities?dataset=ec_meetings&reverse=addr-041ab56007250cb6752559152411948269a968bd"
    )
    data = res.json()
    assert data["total"] == 4
    tested = False
    for entity in data["entities"]:
        assert (
            "addr-041ab56007250cb6752559152411948269a968bd"
            in entity["properties"]["addressEntity"]
        )
        tested = True
    assert tested


def test_api_aggregation():
    res = client.get(
        "/aggregate?dataset=ec_meetings&schema=Event&aggMin=date&aggMax=date"
    )
    data = res.json()
    data.pop("coverage")
    assert data == {
        "total": 34975,
        "query": {
            "limit": 100,
            "page": 1,
            "dataset": ["ec_meetings"],
            "schema": "Event",
            "order_by": None,
            "prop": None,
            "value": None,
            "reverse": None,
            "aggMax": "date",
            "aggMin": "date",
        },
        "url": "http://testserver/aggregate?dataset=ec_meetings&schema=Event&aggMin=date&aggMax=date&limit=100&page=1",
        "aggregations": {"date": {"min": "2014-11-12", "max": "2023-01-20"}},
    }
