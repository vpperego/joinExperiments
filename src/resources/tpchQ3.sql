CREATE TABLE customer (
        c_custkey INTEGER NOT NULL,
        c_name VARCHAR(25) NOT NULL,
        c_address VARCHAR(40) NOT NULL,
        c_nationkey INTEGER NOT NULL,
        c_phone CHAR(15) NOT NULL,
        c_acctbal NUMERIC NOT NULL,
        c_mktsegment CHAR(10) NOT NULL,
        c_comment VARCHAR(117) NOT NULL
);

CREATE TABLE orders (
        o_orderkey BIGINT NOT NULL,
        o_custkey INTEGER NOT NULL,
        o_orderstatus CHAR(1) NOT NULL,
        o_totalprice NUMERIC NOT NULL,
        o_orderdate DATE NOT NULL,
        o_orderpriority CHAR(15) NOT NULL,
        o_clerk CHAR(15) NOT NULL,
        o_shippriority INTEGER NOT NULL,
        o_comment VARCHAR(79) NOT NULL
);

CREATE TABLE lineitem (
        l_orderkey BIGINT NOT NULL,
        l_partkey INTEGER NOT NULL,
        l_suppkey INTEGER NOT NULL,
        l_linenumber INTEGER NOT NULL,
        l_quantity NUMERIC NOT NULL,
        l_extendedprice NUMERIC NOT NULL,
        l_discount NUMERIC NOT NULL,
        l_tax NUMERIC NOT NULL,
        l_returnflag CHAR(1) NOT NULL,
        l_linestatus CHAR(1) NOT NULL,
        l_shipdate DATE NOT NULL,
        l_commitdate DATE NOT NULL,
        l_receiptdate DATE NOT NULL,
        l_shipinstruct CHAR(25) NOT NULL,
        l_shipmode CHAR(10) NOT NULL,
        l_comment VARCHAR(44) NOT NULL
);


select
	l_orderkey,
	sum(l_extendedprice * (1 - l_discount)) as revenue,
	o_orderdate,
	o_shippriority
from
	customer,
	orders,
	lineitem
where
	c_mktsegment = 'FURNITURE'
	and c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and o_orderdate < date '1995-03-29'
	and l_shipdate > date '1995-03-29'
group by
	l_orderkey,
	o_orderdate,
	o_shippriority
order by
	revenue desc,
	o_orderdate
LIMIT 10;