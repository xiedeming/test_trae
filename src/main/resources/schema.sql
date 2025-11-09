-- auto-generated definition
create table bookdb.book
(
    id               bigint auto_increment comment '主键id'
        primary key,
    website_id bigint null comment '书籍站点id',
    book_name        varchar(64)  null comment '书籍名称',
    book_url         varchar(64)  null comment '书籍路径',
    book_summary     varchar(128) null comment '书籍简介',
    create_user_id   bigint          not null comment '书籍创建用户id',
    create_user_name varchar(64)  not null comment '书籍创建用户名称',
    create_time      datetime     not null comment '创建时间',
    modify_user_id   bigint          not null comment '书籍修改用户id',
    modify_user_name varchar(64)  not null comment '书籍修改用户名称',
    modify_time      datetime     not null comment '修改时间'
) comment '书籍表';


create table bookdb.website
(
    id               bigint auto_increment comment '主键id'
        primary key,
    website_name        varchar(64)  null comment '书籍站点名称',
    website_url         varchar(64)  null comment '书籍站点路径',
    website_summary     varchar(128) null comment '书籍站点简介',
    create_user_id   bigint          not null comment '书籍站点创建用户id',
    create_user_name varchar(64)  not null comment '书籍站点创建用户名称',
    create_time      datetime     not null comment '创建时间',
    modify_user_id   bigint          not null comment '书籍站点修改用户id',
    modify_user_name varchar(64)  not null comment '书籍站点修改用户名称',
    modify_time      datetime     not null comment '修改时间'
) comment '书籍站点表';


create table bookdb.user
(
    id               bigint auto_increment comment '主键id'
        primary key,
    user_name        varchar(64)  null comment '用户名称',
    user_pwd         varchar(64)  null comment '用户密码',
    user_salt        varchar(8) null comment '用户密码盐',
    user_type        varchar(8) null comment '用户类型',
    user_role bigint null comment '用户角色id',
    user_email         varchar(64)  null comment '用户邮箱',
    user_phone         varchar(32)  null comment '用户手机号',
    user_summary     varchar(128) null comment '用户简介',
    create_user_id   bigint          not null comment '创建用户id',
    create_user_name varchar(64)  not null comment '创建用户名称',
    create_time      datetime     not null comment '创建时间',
    modify_user_id   bigint          not null comment '修改用户id',
    modify_user_name varchar(64)  not null comment '修改用户名称',
    modify_time      datetime     not null comment '修改时间'
) comment '用户表';


create table bookdb.role
(
    id               bigint auto_increment comment '主键id'
        primary key,
    role_name        varchar(64)  null comment '用户角色名称',
    role_type        varchar(8) null comment '用户角色类型',
    role_summary     varchar(128) null comment '用户角色简介',
    create_user_id   bigint          not null comment '创建用户id',
    create_user_name varchar(64)  not null comment '创建用户名称',
    create_time      datetime     not null comment '创建时间',
    modify_user_id   bigint          not null comment '修改用户id',
    modify_user_name varchar(64)  not null comment '修改用户名称',
    modify_time      datetime     not null comment '修改时间'
) comment '用户角色表';

create table bookdb.menu
(
    id               bigint auto_increment comment '主键id'
        primary key,
    menu_name        varchar(64)  not null comment '菜单名称',
    role_type        varchar(8) not null comment '菜单类型',
    menu_summary     varchar(128) null comment '菜单简介',
    create_user_id   bigint          not null comment '创建用户id',
    create_user_name varchar(64)  not null comment '创建用户名称',
    create_time      datetime     not null comment '创建时间',
    modify_user_id   bigint          not null comment '修改用户id',
    modify_user_name varchar(64)  not null comment '修改用户名称',
    modify_time      datetime     not null comment '修改时间'
) comment '菜单表';

create table bookdb.role_menu
(
    id               bigint auto_increment comment '主键id'
        primary key,
    role_id        bigint not  null comment '用户角色id',
    menu_id       bigint not null comment '菜单id',
    role_menu_summary     varchar(128) null comment '用户角色简介',
    create_user_id   bigint          not null comment '创建用户id',
    create_user_name varchar(64)  not null comment '创建用户名称',
    create_time      datetime     not null comment '创建时间',
    modify_user_id   bigint          not null comment '修改用户id',
    modify_user_name varchar(64)  not null comment '修改用户名称',
    modify_time      datetime     not null comment '修改时间'
) comment '角色菜单表';


create table bookdb.chapter
(
    id               bigint auto_increment comment '主键id'
        primary key,
    book_id        bigint  null comment '书籍id',
    chapter_url         varchar(64)  null comment '书籍章节路径',
    chapter_name         varchar(64)  null comment '书籍章节名称',
    chapter_order_id int null comment '书籍章节排序',
    chapter_txt     text null comment '章节内容',
    create_user_id   bigint          not null comment '章节创建用户id',
    create_user_name varchar(64)  not null comment '章节创建用户名称',
    create_time      datetime     not null comment '创建时间',
    modify_user_id   bigint          not null comment '章节修改用户id',
    modify_user_name varchar(64)  not null comment '章节修改用户名称',
    modify_time      datetime     not null comment '修改时间'
)
    comment '书籍章节表';

