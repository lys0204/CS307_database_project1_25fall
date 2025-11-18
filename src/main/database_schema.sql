create table users (
                       authorid bigint primary key,
                       authorname text not null,
                       gender varchar(10),
                       age integer
);

create table recipes (
                         recipeid bigint primary key,
                         authorid bigint,
                         name text not null,
                         cooktime text,
                         preptime text,
                         datepublished timestamp,
                         description text,
                         recipecategory text,
                         recipeservings integer,
                         recipeyield text,
                         foreign key (authorid) references users(authorid) on delete set null
);

create table reviews (
                         reviewid bigint primary key,
                         recipeid bigint,
                         authorid bigint,
                         rating integer not null,
                         review text,
                         datesubmitted timestamp,
                         datemodified timestamp,
                         foreign key (recipeid) references recipes(recipeid) on delete cascade,
                         foreign key (authorid) references users(authorid) on delete cascade
);

create table nutrition (
                           recipeid bigint primary key,
                           calories numeric(10, 2) not null,
                           fatcontent numeric(10, 2),
                           saturatedfatcontent numeric(10, 2),
                           cholesterolcontent numeric(10, 2),
                           sodiumcontent numeric(10, 2),
                           carbohydratecontent numeric(10, 2),
                           fibercontent numeric(10, 2),
                           sugarcontent numeric(10, 2),
                           proteincontent numeric(10, 2),
                           foreign key (recipeid) references recipes(recipeid) on delete cascade
);

create table instructions (
                              recipeid bigint,
                              stepnumber integer,
                              instructiontext text not null,
                              primary key (recipeid, stepnumber),
                              foreign key (recipeid) references recipes(recipeid) on delete cascade
);

create table ingredients (
                             ingredientid bigint generated always as identity primary key,
                             ingredientname text not null unique
);

create table recipe_ingredients (
                                    recipeid bigint,
                                    ingredientid bigint,
                                    primary key (recipeid, ingredientid),
                                    foreign key (recipeid) references recipes(recipeid) on delete cascade,
                                    foreign key (ingredientid) references ingredients(ingredientid) on delete cascade
);

create table keywords (
                          keywordid bigint generated always as identity primary key,
                          keywordtext text not null unique
);

create table recipe_keywords (
                                 recipeid bigint,
                                 keywordid bigint,
                                 primary key (recipeid, keywordid),
                                 foreign key (recipeid) references recipes(recipeid) on delete cascade,
                                 foreign key (keywordid) references keywords(keywordid) on delete cascade
);

create table user_favorite_recipes (
                                       authorid bigint,
                                       recipeid bigint,
                                       primary key (authorid, recipeid),
                                       foreign key (authorid) references users(authorid) on delete cascade,
                                       foreign key (recipeid) references recipes(recipeid) on delete cascade
);

create table user_liked_reviews (
                                    authorid bigint,
                                    reviewid bigint,
                                    primary key (authorid, reviewid),
                                    foreign key (authorid) references users(authorid) on delete cascade,
                                    foreign key (reviewid) references reviews(reviewid) on delete cascade
);

create table user_follows (
                              followerid bigint,
                              followingid bigint,
                              primary key (followerid, followingid),
                              foreign key (followerid) references users(authorid) on delete cascade,
                              foreign key (followingid) references users(authorid) on delete cascade
);