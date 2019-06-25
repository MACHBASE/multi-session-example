create table syslog_table
(
    tm datetime,
    n1 short,
    n2 int,
    n3 long,
    n4 float,
    n5 double,
    host varchar(128),
    msg varchar(512),
    ip4 ipv4,
    ip6 ipv6
);
