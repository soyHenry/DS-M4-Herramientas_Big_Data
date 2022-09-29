use integrador2;
select idsucursal, sum(precio * cantidad) from venta group by idsucursal;
CREATE INDEX index_venta_sucursal ON TABLE venta(IdSucursal) AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD;