# ftmstore-fastapi

## 2.0.0 (2023-09-28)

- Build on top of [ftmq](https://github.com/investigativedata/ftmq/)
- Drop dataset scope binding:
    - no `/{dataset}/entities` routes anymore, instead: `/entities?dataset={dataset}`
