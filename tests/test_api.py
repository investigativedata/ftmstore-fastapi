from unittest import TestCase

from fastapi.testclient import TestClient

from ftmstore_fastapi.api import app


class ApiTestCase(TestCase):
    client = TestClient(app)
    maxDiff = None
    ec_things = {
        "countries": [],
        "schemata": [
            {
                "count": 1281,
                "label": "Address",
                "name": "Address",
                "plural": "Addresses",
            },
            {
                "count": 34975,
                "label": "Event",
                "name": "Event",
                "plural": "Events",
            },
            {
                "count": 791,
                "label": "Membership",
                "name": "Membership",
                "plural": "Memberships",
            },
            {
                "count": 7097,
                "label": "Organization",
                "name": "Organization",
                "plural": "Organizations",
            },
            {
                "count": 791,
                "label": "Person",
                "name": "Person",
                "plural": "People",
            },
            {
                "count": 103,
                "label": "Public body",
                "name": "PublicBody",
                "plural": "Public bodies",
            },
        ],
        "total": 45038,
    }
    gdho_things = {
        "countries": [
            {"code": "ad", "count": 1, "label": "Andorra"},
            {"code": "ae", "count": 1, "label": "United Arab Emirates"},
            {"code": "af", "count": 239, "label": "Afghanistan"},
            {"code": "ag", "count": 1, "label": "Antigua and Barbuda"},
            {"code": "al", "count": 2, "label": "Albania"},
            {"code": "am", "count": 4, "label": "Armenia"},
            {"code": "ao", "count": 2, "label": "Angola"},
            {"code": "ar", "count": 2, "label": "Argentina"},
            {"code": "at", "count": 6, "label": "Austria"},
            {"code": "au", "count": 15, "label": "Australia"},
            {"code": "az", "count": 6, "label": "Azerbaijan"},
            {"code": "ba", "count": 5, "label": "Bosnia and Herzegovina"},
            {"code": "bb", "count": 1, "label": "Barbados"},
            {"code": "bd", "count": 148, "label": "Bangladesh"},
            {"code": "be", "count": 34, "label": "Belgium"},
            {"code": "bf", "count": 33, "label": "Burkina Faso"},
            {"code": "bg", "count": 6, "label": "Bulgaria"},
            {"code": "bh", "count": 1, "label": "Bahrain"},
            {"code": "bi", "count": 24, "label": "Burundi"},
            {"code": "bj", "count": 3, "label": "Benin"},
            {"code": "bn", "count": 1, "label": "Brunei Darussalam"},
            {
                "code": "bo",
                "count": 12,
                "label": "Bolivia, Plurinational " "State of",
            },
            {"code": "br", "count": 8, "label": "Brazil"},
            {"code": "bs", "count": 1, "label": "Bahamas"},
            {"code": "bw", "count": 1, "label": "Botswana"},
            {"code": "by", "count": 4, "label": "Belarus"},
            {"code": "bz", "count": 2, "label": "Belize"},
            {"code": "ca", "count": 16, "label": "Canada"},
            {
                "code": "cd",
                "count": 64,
                "label": "Congo, The Democratic " "Republic of the",
            },
            {"code": "cf", "count": 95, "label": "Central African Republic"},
            {"code": "cg", "count": 4, "label": "Congo"},
            {"code": "ch", "count": 36, "label": "Switzerland"},
            {"code": "ci", "count": 10, "label": "Côte d'Ivoire"},
            {"code": "ck", "count": 1, "label": "Cook Islands"},
            {"code": "cl", "count": 4, "label": "Chile"},
            {"code": "cm", "count": 46, "label": "Cameroon"},
            {"code": "cn", "count": 17, "label": "China"},
            {"code": "co", "count": 168, "label": "Colombia"},
            {"code": "cr", "count": 3, "label": "Costa Rica"},
            {"code": "csxx", "count": 8, "label": None},
            {"code": "cu", "count": 3, "label": "Cuba"},
            {"code": "cv", "count": 1, "label": "Cabo Verde"},
            {"code": "cy", "count": 3, "label": "Cyprus"},
            {"code": "cz", "count": 3, "label": "Czechia"},
            {"code": "de", "count": 32, "label": "Germany"},
            {"code": "dj", "count": 4, "label": "Djibouti"},
            {"code": "dk", "count": 10, "label": "Denmark"},
            {"code": "do", "count": 4, "label": "Dominican Republic"},
            {"code": "dz", "count": 3, "label": "Algeria"},
            {"code": "ec", "count": 9, "label": "Ecuador"},
            {"code": "ee", "count": 2, "label": "Estonia"},
            {"code": "eg", "count": 14, "label": "Egypt"},
            {"code": "es", "count": 19, "label": "Spain"},
            {"code": "et", "count": 59, "label": "Ethiopia"},
            {"code": "fi", "count": 3, "label": "Finland"},
            {"code": "fj", "count": 1, "label": "Fiji"},
            {
                "code": "fm",
                "count": 1,
                "label": "Micronesia, Federated " "States of",
            },
            {"code": "fr", "count": 62, "label": "France"},
            {"code": "ga", "count": 1, "label": "Gabon"},
            {"code": "gb", "count": 75, "label": "United Kingdom"},
            {"code": "gd", "count": 1, "label": "Grenada"},
            {"code": "ge", "count": 6, "label": "Georgia"},
            {"code": "gh", "count": 8, "label": "Ghana"},
            {"code": "gm", "count": 4, "label": "Gambia"},
            {"code": "gn", "count": 8, "label": "Guinea"},
            {"code": "gq", "count": 1, "label": "Equatorial Guinea"},
            {"code": "gr", "count": 8, "label": "Greece"},
            {"code": "gt", "count": 22, "label": "Guatemala"},
            {"code": "gw", "count": 4, "label": "Guinea-Bissau"},
            {"code": "gy", "count": 1, "label": "Guyana"},
            {"code": "hn", "count": 14, "label": "Honduras"},
            {"code": "hr", "count": 7, "label": "Croatia"},
            {"code": "ht", "count": 65, "label": "Haiti"},
            {"code": "hu", "count": 3, "label": "Hungary"},
            {"code": "id", "count": 31, "label": "Indonesia"},
            {"code": "ie", "count": 4, "label": "Ireland"},
            {"code": "il", "count": 8, "label": "Israel"},
            {"code": "in", "count": 32, "label": "India"},
            {"code": "iq", "count": 97, "label": "Iraq"},
            {"code": "ir", "count": 4, "label": "Iran, Islamic Republic of"},
            {"code": "is", "count": 1, "label": "Iceland"},
            {"code": "it", "count": 74, "label": "Italy"},
            {"code": "jm", "count": 1, "label": "Jamaica"},
            {"code": "jo", "count": 7, "label": "Jordan"},
            {"code": "jp", "count": 18, "label": "Japan"},
            {"code": "ke", "count": 33, "label": "Kenya"},
            {"code": "kg", "count": 3, "label": "Kyrgyzstan"},
            {"code": "kh", "count": 7, "label": "Cambodia"},
            {"code": "ki", "count": 1, "label": "Kiribati"},
            {"code": "km", "count": 1, "label": "Comoros"},
            {"code": "kn", "count": 1, "label": "Saint Kitts and Nevis"},
            {
                "code": "kp",
                "count": 1,
                "label": "Korea, Democratic People's " "Republic of",
            },
            {"code": "kr", "count": 1, "label": "Korea, Republic of"},
            {"code": "kw", "count": 1, "label": "Kuwait"},
            {"code": "kz", "count": 4, "label": "Kazakhstan"},
            {
                "code": "la",
                "count": 2,
                "label": "Lao People's Democratic " "Republic",
            },
            {"code": "lb", "count": 21, "label": "Lebanon"},
            {"code": "lc", "count": 1, "label": "Saint Lucia"},
            {"code": "li", "count": 1, "label": "Liechtenstein"},
            {"code": "lk", "count": 9, "label": "Sri Lanka"},
            {"code": "lr", "count": 7, "label": "Liberia"},
            {"code": "ls", "count": 4, "label": "Lesotho"},
            {"code": "lt", "count": 1, "label": "Lithuania"},
            {"code": "lu", "count": 2, "label": "Luxembourg"},
            {"code": "lv", "count": 2, "label": "Latvia"},
            {"code": "ma", "count": 5, "label": "Morocco"},
            {"code": "md", "count": 3, "label": "Moldova, Republic of"},
            {"code": "mg", "count": 8, "label": "Madagascar"},
            {"code": "mk", "count": 3, "label": "North Macedonia"},
            {"code": "ml", "count": 97, "label": "Mali"},
            {"code": "mm", "count": 132, "label": "Myanmar"},
            {"code": "mn", "count": 1, "label": "Mongolia"},
            {"code": "mr", "count": 6, "label": "Mauritania"},
            {"code": "mt", "count": 2, "label": "Malta"},
            {"code": "mu", "count": 1, "label": "Mauritius"},
            {"code": "mv", "count": 1, "label": "Maldives"},
            {"code": "mw", "count": 40, "label": "Malawi"},
            {"code": "mx", "count": 5, "label": "Mexico"},
            {"code": "my", "count": 8, "label": "Malaysia"},
            {"code": "mz", "count": 26, "label": "Mozambique"},
            {"code": "na", "count": 1, "label": "Namibia"},
            {"code": "ne", "count": 28, "label": "Niger"},
            {"code": "ng", "count": 43, "label": "Nigeria"},
            {"code": "ni", "count": 3, "label": "Nicaragua"},
            {"code": "nl", "count": 15, "label": "Netherlands"},
            {"code": "no", "count": 10, "label": "Norway"},
            {"code": "np", "count": 32, "label": "Nepal"},
            {"code": "nz", "count": 3, "label": "New Zealand"},
            {"code": "pa", "count": 3, "label": "Panama"},
            {"code": "pe", "count": 8, "label": "Peru"},
            {"code": "pg", "count": 3, "label": "Papua New Guinea"},
            {"code": "ph", "count": 27, "label": "Philippines"},
            {"code": "pk", "count": 176, "label": "Pakistan"},
            {"code": "pl", "count": 7, "label": "Poland"},
            {"code": "pt", "count": 5, "label": "Portugal"},
            {"code": "pw", "count": 1, "label": "Palau"},
            {"code": "py", "count": 2, "label": "Paraguay"},
            {"code": "qa", "count": 2, "label": "Qatar"},
            {"code": "ro", "count": 4, "label": "Romania"},
            {"code": "ru", "count": 9, "label": "Russian Federation"},
            {"code": "rw", "count": 4, "label": "Rwanda"},
            {"code": "sa", "count": 2, "label": "Saudi Arabia"},
            {"code": "sb", "count": 2, "label": "Solomon Islands"},
            {"code": "sc", "count": 1, "label": "Seychelles"},
            {"code": "sd", "count": 96, "label": "Sudan"},
            {"code": "se", "count": 16, "label": "Sweden"},
            {"code": "sg", "count": 1, "label": "Singapore"},
            {"code": "si", "count": 2, "label": "Slovenia"},
            {"code": "sk", "count": 5, "label": "Slovakia"},
            {"code": "sl", "count": 39, "label": "Sierra Leone"},
            {"code": "sm", "count": 1, "label": "San Marino"},
            {"code": "sn", "count": 8, "label": "Senegal"},
            {"code": "so", "count": 262, "label": "Somalia"},
            {"code": "sr", "count": 1, "label": "Suriname"},
            {"code": "ss", "count": 117, "label": "South Sudan"},
            {"code": "st", "count": 6, "label": "Sao Tome and Principe"},
            {"code": "sv", "count": 7, "label": "El Salvador"},
            {"code": "sy", "count": 70, "label": "Syrian Arab Republic"},
            {"code": "sz", "count": 2, "label": "Eswatini"},
            {"code": "td", "count": 58, "label": "Chad"},
            {"code": "tg", "count": 5, "label": "Togo"},
            {"code": "th", "count": 11, "label": "Thailand"},
            {"code": "tj", "count": 3, "label": "Tajikistan"},
            {"code": "tl", "count": 6, "label": "Timor-Leste"},
            {"code": "tm", "count": 3, "label": "Turkmenistan"},
            {"code": "tn", "count": 3, "label": "Tunisia"},
            {"code": "to", "count": 1, "label": "Tonga"},
            {"code": "tr", "count": 5, "label": "Turkey"},
            {"code": "tt", "count": 2, "label": "Trinidad and Tobago"},
            {"code": "tz", "count": 20, "label": "Tanzania, United Republic " "of"},
            {"code": "ua", "count": 151, "label": "Ukraine"},
            {"code": "ug", "count": 13, "label": "Uganda"},
            {"code": "us", "count": 314, "label": "United States"},
            {"code": "uy", "count": 2, "label": "Uruguay"},
            {"code": "uz", "count": 1, "label": "Uzbekistan"},
            {"code": "va", "count": 1, "label": "Holy See (Vatican City " "State)"},
            {
                "code": "vc",
                "count": 1,
                "label": "Saint Vincent and the " "Grenadines",
            },
            {
                "code": "ve",
                "count": 9,
                "label": "Venezuela, Bolivarian " "Republic of",
            },
            {"code": "vn", "count": 4, "label": "Viet Nam"},
            {"code": "vu", "count": 1, "label": "Vanuatu"},
            {"code": "ws", "count": 1, "label": "Samoa"},
            {"code": "ye", "count": 109, "label": "Yemen"},
            {"code": "za", "count": 17, "label": "South Africa"},
            {"code": "zm", "count": 5, "label": "Zambia"},
            {"code": "zw", "count": 52, "label": "Zimbabwe"},
        ],
        "schemata": [
            {
                "count": 4633,
                "label": "Organization",
                "name": "Organization",
                "plural": "Organizations",
            }
        ],
        "total": 4633,
    }

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
                        "category": None,
                        "name": "ec_meetings",
                        "title": "Ec Meetings",
                        "summary": None,
                        "url": None,
                        "load": False,
                        "entities_url": "http://testserver/ec_meetings/entities",
                        "version": "1",
                        "children": [],
                        "frequency": None,
                        "publisher": None,
                        "resources": [],
                        "updated_at": None,
                        "things": self.ec_things,
                    },
                    {
                        "category": None,
                        "name": "eu_authorities",
                        "title": "Eu Authorities",
                        "summary": None,
                        "url": None,
                        "load": False,
                        "entities_url": "http://testserver/eu_authorities/entities",
                        "version": "1",
                        "children": [],
                        "frequency": None,
                        "publisher": None,
                        "resources": [],
                        "updated_at": None,
                        "things": {
                            "countries": [],
                            "schemata": [
                                {
                                    "count": 151,
                                    "label": "Public body",
                                    "name": "PublicBody",
                                    "plural": "Public bodies",
                                }
                            ],
                            "total": 151,
                        },
                    },
                    {
                        "category": None,
                        "name": "gdho",
                        "title": "Gdho",
                        "summary": None,
                        "url": None,
                        "load": False,
                        "entities_url": "http://testserver/gdho/entities",
                        "version": "1",
                        "children": [],
                        "frequency": None,
                        "publisher": None,
                        "resources": [],
                        "updated_at": None,
                        "things": self.gdho_things,
                    },
                ],
                "updated_at": None,
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
                "entities_url": "http://testserver/ec_meetings/entities",
                "resources": [],
                "updated_at": None,
                "version": "1",
                "frequency": None,
                "publisher": None,
                "category": None,
                "children": [],
                "things": self.ec_things,
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

        res = self.client.get("/eu_authorities/entities?jurisdiction=eu&order_by=-name")
        self.assertEqual(200, res.status_code)
        res = res.json()
        self.assertEqual(res["total"], 151)

        res = self.client.get(
            "/eu_authorities/entities?jurisdiction__not=eu&order_by=-name"
        )
        self.assertEqual(200, res.status_code)
        res = res.json()
        self.assertEqual(res["total"], 0)

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
        res = self.client.get("/eu_authorities/entities?q=germany&dehydrate=1")
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

    def test_api_aggregation(self):
        res = self.client.get(
            "/ec_meetings/aggregate?schema=Event&aggMin=date&aggMax=date"
        )
        res = res.json()
        self.assertDictEqual(
            res,
            {
                "total": 34975,
                "query": {
                    "limit": 100,
                    "page": 1,
                    "schema": "Event",
                    "order_by": None,
                    "prop": None,
                    "value": None,
                },
                "url": "http://testserver/ec_meetings/aggregate?schema=Event&aggMin=date&aggMax=date&limit=100&page=1",
                "aggregations": {"date": {"min": "2014-11-12", "max": "2023-01-20"}},
            },
        )
