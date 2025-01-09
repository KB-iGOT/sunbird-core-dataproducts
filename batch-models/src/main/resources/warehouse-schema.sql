-- Create the 'warehouse' database
CREATE DATABASE warehouse;

-- Connect to the 'warehouse' database
\c warehouse;

-- Table: user_detail
CREATE TABLE user_detail( 
   user_id VARCHAR(255),
   mdo_id  VARCHAR(255),
   full_name VARCHAR(255),
   designation VARCHAR(255),
   email VARCHAR(255),
   phone_number VARCHAR(255),
   groups VARCHAR(255),
   tag VARCHAR(512),
   roles VARCHAR(500),
   gender VARCHAR(255),
   category VARCHAR(255),
   created_by_id VARCHAR(255),
   external_system VARCHAR(255),
   external_system_id VARCHAR(255),
   is_verified_karmayogi VARCHAR(255),
   mdo_created_on DATE,
   user_registration_date VARCHAR(255),
   marked_as_not_my_user VARCHAR(255),
   data_last_generated_on VARCHAR(255)
);


-- Table: org_hierarchy
CREATE TABLE org_hierarchy(
   mdo_id VARCHAR(255) PRIMARY KEY NOT NULL,
   mdo_name  VARCHAR(255),
   is_content_provider VARCHAR(64),
   ministry VARCHAR(255),
   department VARCHAR(255),
   organization VARCHAR(255),
   data_last_generated_on VARCHAR(255)
);

-- Table: user_enrolment
CREATE TABLE user_enrolments(
   user_id VARCHAR(255) NOT NULL,
   batch_id VARCHAR(255) NOT NULL,
   content_id  VARCHAR(255)  NOT NULL,
   content_progress_percentage FLOAT,
   last_accessed_on VARCHAR(255) ,
   certificate_generated VARCHAR(255),
   certificate_generated_on VARCHAR(255) ,
   user_rating FLOAT,
   resource_count_consumed INTEGER,
   enrolled_on VARCHAR(255) ,
   user_consumption_status VARCHAR(255),
   data_last_generated_on VARCHAR(255),
   PRIMARY KEY (user_id, content_id, batch_id)
);

-- Table: content
CREATE TABLE content(
   content_id VARCHAR(255) PRIMARY KEY NOT NULL,
   content_provider_id VARCHAR(255),
   content_provider_name VARCHAR(255),
   content_name TEXT,
   content_type VARCHAR(255),
   batch_id VARCHAR(255),
   batch_name VARCHAR(255),
   batch_start_date VARCHAR(255),
   batch_end_date VARCHAR(255),
   content_duration VARCHAR(255),
   content_rating FLOAT,
   last_published_on VARCHAR(255),
   content_retired_on VARCHAR(255),
   content_status VARCHAR(255),
   resource_count INTEGER,
   total_certificates_issued INTEGER,
   content_substatus VARCHAR(255),
   data_last_generated_on VARCHAR(255)
);

-- Table: assessment_detail
CREATE TABLE assessment_detail(
   user_id VARCHAR(255)  NOT NULL,
   content_id VARCHAR(255) NOT NULL,
   assessment_id VARCHAR(255) NOT NULL,
   assessment_name VARCHAR(255) ,
   assessment_type VARCHAR(255),
   assessment_duration VARCHAR(255),
   time_spent_by_the_user VARCHAR(255),
   completion_date VARCHAR(255),
   score_achieved FLOAT,
   overall_score FLOAT,
   cut_off_percentage FLOAT,
   total_question INTEGER,
   number_of_incorrect_responses INTEGER,
   number_of_retakes INTEGER,
   data_last_generated_on VARCHAR(255),
   PRIMARY KEY (assessment_id, content_id, user_id)
);

-- Table: bp_enrollments
CREATE TABLE bp_enrolments(
   user_id VARCHAR(255) NOT NULL,
   content_id  VARCHAR(255) NOT NULL,
   batch_id VARCHAR(255) NOT NULL,
   batch_location VARCHAR(255),
   component_name VARCHAR(255),
   component_id VARCHAR(255),
   component_type VARCHAR(255),
   component_mode VARCHAR(255),
   component_status VARCHAR(255),
   component_duration VARCHAR(255),
   component_progress_percentage FLOAT,
   component_completed_on DATE,
   last_accessed_on DATE,
   offline_session_date DATE,
   offline_session_start_time VARCHAR(255),
   offline_session_end_time VARCHAR(255),
   offline_attendance_status VARCHAR(255),
   instructors_name VARCHAR(255),
   program_coordinator_name VARCHAR(255),
   data_last_generated_on VARCHAR(255)
);

-- Table: cb_plan
CREATE TABLE cb_plan(
   cb_plan_id VARCHAR(255) NOT NULL,
   org_id VARCHAR(255) NOT NULL,
   created_by  VARCHAR(255) NOT NULL,
   plan_name VARCHAR(255),
   allotment_type VARCHAR(255),
   allotment_to VARCHAR(255),
   content_id VARCHAR(255),
   allocated_on VARCHAR(255),
   due_by VARCHAR(255),
   status VARCHAR(255)
);

-- Table: kcm_content_mapping
CREATE TABLE kcm_content_mapping(
   course_id VARCHAR(255) NOT NULL,
   competency_area_id INTEGER,
   competency_theme_id  INTEGER,
   competency_sub_theme_id INTEGER
);

-- Table: kcm_dictionary
CREATE TABLE kcm_dictionary(
   competency_area_id INTEGER,
   competency_area TEXT,
   competency_area_description  TEXT,
   competency_theme_type VARCHAR(255),
   competency_theme_id INTEGER,
   competency_theme TEXT,
   competency_theme_description TEXT,
   competency_sub_theme_id INTEGER,
   competency_sub_theme TEXT,
   competency_sub_theme_description TEXT
);

-- Table: events_table
CREATE TABLE events(
   event_id VARCHAR(255) PRIMARY KEY NOT NULL,
   event_name TEXT,
   event_provider_mdo_id  VARCHAR(255),
   event_start_datetime VARCHAR(255),
   duration VARCHAR(255),
   event_status VARCHAR(255),
   event_type VARCHAR(255),
   presenters TEXT,
   video_link TEXT,
   recording_link TEXT
);

-- Table: events_enrolment_table
CREATE TABLE events_enrolment(
   user_id VARCHAR(255),
   event_id VARCHAR(255),
   enrolled_on_datetime  VARCHAR(255),
   status VARCHAR(255),
   certificate_id TEXT,
   completion_percentage FLOAT,
   PRIMARY KEY(user_id, event_id)
);

-- Create PostgreSQL user and grant privileges
CREATE USER postgres WITH PASSWORD 'Password@12345678';

GRANT SELECT, INSERT, UPDATE, DELETE ON user_detail TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON org_hierarchy TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON user_enrolments TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON content TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON assessment_detail TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON bp_enrolments TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON cb_plan TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON events TO postgres;
GRANT SELECT, INSERT, UPDATE, DELETE ON events_enrolment TO postgres;

ALTER TABLE user_enrolments ADD COLUMN live_cbp_plan_mandate boolean;
ALTER TABLE user_enrolments ADD COLUMN certificate_id VARCHAR(255);
ALTER TABLE user_enrolments ADD COLUMN first_completed_on VARCHAR(255);
ALTER TABLE user_detail ADD COLUMN weekly_claps_day_before_yesterday VARCHAR(255);
ALTER TABLE user_detail ADD COLUMN status INTEGER;
ALTER TABLE user_detail ADD COLUMN no_of_karma_points INTEGER;
ALTER TABLE org_hierarchy ADD COLUMN mdo_created_on DATE;
ALTER TABLE content ADD COLUMN language VARCHAR(255);
ALTER TABLE user_detail ADD COLUMN marked_as_not_my_user boolean;
ALTER TABLE user_detail ADD COLUMN total_event_learning_hours DOUBLE PRECISION;
ALTER TABLE user_detail ADD COLUMN total_content_learning_hours DOUBLE PRECISION;
ALTER TABLE user_detail ADD COLUMN total_learning_hours DOUBLE PRECISION;
ALTER TABLE events_enrolment ADD COLUMN event_duration character varying(255);
ALTER TABLE events_enrolment ADD COLUMN progress_duration character varying(255);
ALTER TABLE events_enrolment ADD COLUMN completed_on_datetime character varying(255);
ALTER TABLE events_enrolment ADD COLUMN duration character varying(255);
alter table user_detail ALTER COLUMN is_verified_karmayogi type character varying(15);

CREATE TABLE user_activity(
   user_id VARCHAR(255) NOT NULL,
   batch_id VARCHAR(255) NOT NULL,
   content_id  VARCHAR(255)  NOT NULL,
   content_type character varying(255),
   content_progress_percentage double precision,
   certificate_generated character varying(255),
   user_rating double precision,
   resource_count_consumed INTEGER,
   enrolled_on character varying(255),
   user_consumption_status character varying(255),
   data_last_generated_on character varying(255),
   certificate_id character varying(255),
   live_cbp_plan_mandate boolean,
   first_completed_on character varying(255),
   last_completed_on character varying(255),
   first_certificate_generated_on character varying(255),
   last_certificate_generated_on character varying(255),
   content_last_accessed_on character varying(255),
   number_of_certificate character varying(255),
   PRIMARY KEY (user_id, content_id, batch_id)
);

GRANT SELECT, INSERT, UPDATE, DELETE ON user_enrolments TO postgres;

ALTER TABLE user_detail RENAME COLUMN is_verified_karmayogi TO profile_status;
ALTER TABLE user_detail ALTER COLUMN profile_status TYPE character varying(15);
ALTER TABLE user_detail ADD COLUMN employee_id VARCHAR(100);

alter table user_enrolments drop column marked_as_not_my_user;
-- adding new column in content
ALTER TABLE content ADD COLUMN content_sub_type character varying(50);

-- competency v6 ids are not integer
ALTER TABLE kcm_content_mapping ALTER COLUMN competency_area_id TYPE VARCHAR(255) USING competency_area_id::VARCHAR(255);
ALTER TABLE kcm_content_mapping ALTER COLUMN competency_theme_id TYPE VARCHAR(255) USING competency_theme_id::VARCHAR(255);
ALTER TABLE kcm_content_mapping ALTER COLUMN competency_sub_theme_id TYPE VARCHAR(255) USING competency_sub_theme_id::VARCHAR(255);
ALTER TABLE kcm_dictionary ALTER COLUMN competency_area_id TYPE VARCHAR(255) USING competency_area_id::VARCHAR(255);
ALTER TABLE kcm_dictionary ALTER COLUMN competency_theme_id TYPE VARCHAR(255) USING competency_theme_id::VARCHAR(255);
ALTER TABLE kcm_dictionary ALTER COLUMN competency_sub_theme_id TYPE VARCHAR(255) USING competency_sub_theme_id::VARCHAR(255);