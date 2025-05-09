create table adult_dataset (
    id integer primary key autoincrement,
    age text not null,
    marital_status text not null,
    hours_per_week text not null,
    education text not null,
    work_sector text not null,
    occupation text not null,
    race text not null,
    sex text not null,
    income text not null
);