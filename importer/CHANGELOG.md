# CHANGES
changes in descending order by version number

## version 2.1.4
`09.11.2020`

- Machine type - handle empty "machine_type_id" column

## version 2.1.3
`04.11.2020`

- Machine type (is_default) - solved bugfix for the "action 50" operation

## version 2.1.2
`04.11.2020`

- Machine type - not mandatory for machine import

## version 2.1.1
`04.11.2020`

- Fix on default machine type

## version 2.1.0
`04.11.2020`

- Handle planogram export and import adjustments (machine and machine type) for Hungary

## version 2.0.22
`04.11.2020`

- Improved handling duplicate combo code on specific product

## version 2.0.21

`03.11.2020`

- Handle product with duplicate combo recipe code 

## version 2.0.20

`03.11.2020`

- Handle product with duplicate recipe code 


## version 2.0.19

`03.11.2020`

- Handle product rotation groups on planogram import
- Handle duplicate product code on planogram import


## version 2.0.0

`22.09.2020`

- This version brings significant import speed & optimisation on planogram & machine type.
- planogram & machine type become a stand alone import service (planogram & machine type don't use `CLOUD` any more) 
- planogram & machine has his own `PL/SQL procedure` 
- added `BaseImporter` and `populate` (generic way of building import objects, it can be used on any import type)
- added `save`, importer call PL/SQL procedure and send objects for import
- added `PL/pgSQL procedure` for planogram & machine type
- `PL/pgSQL procedure` use temp tables and make action od database in one transaction



