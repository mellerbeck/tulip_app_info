CREATE TABLE tulipappstructure.tulip_info_header (
	app_id varchar(50) NOT NULL,
	persistent_id varchar(50) NOT NULL,
	process_id varchar(50) NOT NULL,
	"name" varchar(250) NOT NULL,
	group_id varchar(50) NOT NULL,
	group_name varchar(250) NOT NULL,
	instance_path varchar(250) NOT NULL,
	created_at timestamptz NOT NULL,
	new_column varchar(100) NULL,
	CONSTRAINT tulip_info_header_pk PRIMARY KEY (persistent_id, process_id)
);

CREATE TABLE tulipappstructure.tulip_info_details (
	persistent_id varchar(50) NOT NULL,
	process_id varchar(50) NOT NULL,
	function_id varchar(50) NOT NULL,
	function_name varchar(250) NOT NULL,
	function_query text NOT NULL,
	trigger_id varchar(50) NOT NULL,
	trigger_name varchar(250) NULL,
	parent_step varchar(50) NULL,
	step_name varchar(250) NULL,
	widget_id varchar(50) NULL,
	widget_type varchar(50) NULL,
	direct_link varchar(1000) NULL,
	created_at timestamptz NULL,
	CONSTRAINT tulip_info_details_pk PRIMARY KEY (persistent_id, process_id, function_id, trigger_id),
	CONSTRAINT tulipinfodetails_fkey FOREIGN KEY (persistent_id,process_id) REFERENCES tulipappstructure.tulip_info_header(persistent_id,process_id)
);
