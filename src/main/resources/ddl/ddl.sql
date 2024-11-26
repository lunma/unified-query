create table if not exists datasource_management(
    id int primary key auto_increment,
    schema_name varchar(255),
    driver varchar(255),
    jdbc_url varchar(255),
    username varchar(255),
    password varchar(255),
    unique(schema_name,driver)
)