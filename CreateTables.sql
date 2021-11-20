create table if not exists course(
    id                  varchar(10)     primary key,
    course_hour         int             not null,
    course_credit       float           not null,
    name                varchar(50)     not null,
    course_dept         varchar(30)     not null,
    pre_course_names    varchar(50)[]           ,
    truth_table         boolean[][]
);
create table if not exists class(
    id          serial      primary key,
    name        varchar(30) not null,
    course_id   varchar(10) not null    references course(id),
    unique(name,course_id)
);
create table if not exists class_list(
    id          serial      primary key,
    class_id    int         not null    references class(id),
    location    varchar(30) not null,
    weekday     int         not null,
    start_class int         not null,
    end_class   int         not null,
    week_list   int[]       not null,
    unique(class_id,weekday,start_class,end_class,week_list)
);
create table if not exists teacher(
    id      serial      primary key,
    name    varchar(40) not null
);
create table if not exists class_teacher(
    class_id    int references class(id),
    teacher_id  int references teacher(id),
    primary key(class_id,teacher_id)
);
create table if not exists student(
    id                      int         primary key,
    gender                  char(1)     not null,
    chinese_surname         char(1)     not null,
    chinese_given_name      char(2)     not null,
    english_name            varchar(20) not null,
    translated_english_name char(4)     not null
);
create table if not exists course_selection(
    course_id   varchar(20) references course(id),
    student_id  int         references student(id),
    primary key(course_id,student_id)
);