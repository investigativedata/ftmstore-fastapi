# ftmstore-fastapi

## 2.0.1 (2023-10-19)

- Drop support for Python 3.10 due to dependencies
- Implement aggregation endpoints
- Use store resolver

## 2.0.0 (2023-09-28)

- Build on top of [ftmq](https://github.com/investigativedata/ftmq/)
- Drop dataset scope binding:
    - no `/{dataset}/entities` routes anymore, instead: `/entities?dataset={dataset}`
