create table if not exists symbol_settings (
    id int not null auto_increment primary key,
    symbol varchar(50) not null,
    indicator varchar(50) not null,
    amplitude varchar(10) not null,
    hyperopted_balance varchar(50),
    created_at datetime not null,
    updated_at datetime
);