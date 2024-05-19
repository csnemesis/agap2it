--1. Primary key definition and any other constraint or index suggestion
-- Adicionando PK as tabelas e FK
ALTER table item
ADD CONSTRAINT pk_item PRIMARY KEY (item,dept);

ALTER table loc
ADD CONSTRAINT pk_loc PRIMARY KEY (loc);

ALTER table item_loc_soh
ADD CONSTRAINT pk_item_loc_soh PRIMARY KEY (item,loc,dept);


--2. Your suggestion for table data management and data access considering the application usage, for example, partition...
-- primeira sugestao seria por 100 registros, se nao tiver performance adicionar por loc e dept
drop table item_loc_soh;

create table item_loc_soh(
item varchar2(25) not null primary key,
loc number(10) not null ,
dept number(4) not null ,
unit_cost number(20,4) not null,
stock_on_hand number(12,4) not null
)   
  PARTITION BY RANGE (loc) INTERVAL (100)
  (
      partition p1 values less than (100)
  )
;

ALTER table item_loc_soh
ADD CONSTRAINT pk_item_loc_soh PRIMARY KEY (item,loc,dept);

--3. Your suggestion to avoid row contention at table level parameter because of high level of concurrency
-- Otimizar as consultas, validar se os indices que estão criados estao sendo usados corretamente, caso negativo alteralos e verificar se é necessario criar novos, verificar se existe partition para as tabelas que estao sendo usadas.

--4. Create a view that can be used at screen level to show only the required fields
-- quais são os required fields? segue um exemplo

Create or replace view v_item_loc_soh as 
 select item, loc, dept, unit_cost, stock_on_hand
  from item_loc_soh;
  
--5. Create a new table that associates user to existing dept(s)
-- seria melhor ter mais informações, tal como se ele pode acessar mais de um dept, para criar a tabela que atenda melhor a necessidade
create table user_dept (
    user_id varchar2(25) not null,
    dept number(4) not null
);

--6. Create a package with procedure or function that can be invoked by store or all stores to save the item_loc_soh to a new table that will contain the same information plus the stock value per item/loc (unit_cost*stock_on_hand)
-- tabela para receber os registros

create table item_loc_soh_hist(
item varchar2(25) not null,
loc number(10) not null,
dept number(4) not null,
unit_cost number(20,4) not null,
stock_on_hand number(12,4) not null,
stock_value number(20,4)
);

CREATE OR REPLACE PACKAGE process_item_loc_soh AS 

FUNCTION move_history (i_store number) 
   RETURN boolean; 
   
END process_item_loc_soh; 

CREATE OR REPLACE PACKAGE BODY process_item_loc_soh is 

  FUNCTION move_history (i_store number) 
    RETURN boolean IS 
	
  insert into item_loc_soh_hist 
  SELECT item,loc,dept,unit_cost,stock_on_hand, unit_cost*stock_on_hand
    FROM item_loc_soh
    WHERE (loc = i_store OR loc > 0);
	
  end move_history;	
BEGIN
  move_history(i_store);
  return true;
  exception 
   return false;  
END process_item_loc_soh; 

--7. Create a data filter mechanism that can be used at screen level to filter out the data that user can see accordingly to dept association (created previously)

  FUNCTION list_users_dept (i_user_id varchar2(25)) 
    RETURN out sys_refcursor IS 
	
	o_cursor   SYS_REFCURSOR;
  
	BEGIN
	   OPEN o_cursor FOR
		    SELECT dept 
              FROM user_dept
             WHERE user_id = i_user_id;

	   RETURN o_cursor;
	END;  
	
  end move_history;	


--8. Create a pipeline function to be used in the location list of values (drop down).  ate o momento nao havia utilizado a function pipelined 
create or replace package pck_pipelined
is

type typ_loc_list is record (i_loc number);

type typ_tb_loc_list is table of typ_loc_list;   

end pck_pipelined;

create or replace package body pck_pipelined is

	function return_loc
	return typ_tb_loc_list pipelined
	is

	  v_record          typ_loc_list;

	  CURSOR c_data IS
		SELECT loc
		FROM   loc;

	begin
	  FOR cur_rec IN c_data LOOP
	  
		v_record := cur_rec.loc;
		pipe row (v_record);
		
	  END LOOP;
	  
	  RETURN;
	end return_loc;
end pck_pipelined;
----------2222222222222222

--9. Looking into the following explain plan what should be your recommendation and implementation to improve the existing data model. Please share your solution in sql and the corresponding explain plan of that solution. Please take in consideration the way that user will use the app. 
--A partida a tabela não foi PK e indices, sugestao seria criar pk e indices para a tabela

ALTER table item_loc_soh
ADD CONSTRAINT pk_item_loc_soh PRIMARY KEY (item,loc,dept);
CREATE INDEX EMP_NAME_IX ON EMPLOYEES (item,loc,dept);

-- 10. Run the previous method that was created on 6. for all the stores from item_loc_soh to the history table. The entire migration should not take more than 10s to run (don't use parallel hint to solve it :)) 
--  esta a dar o erro ao inserir os registros ORA-01536: space quota exceeded for tablespace 'APEX_BIGFILE_INSTANCE_TBS5' não consegui ter os total de dados

--11. Please have a look into the AWR report (AWR.html) in attachment and let us know what is the problem that the AWR is highlighting and potential solution.
-- DB time esta alto, ha muito tempo de espera pelos utilizadores. O valor resmgr:cpu quantum esta alto. Tamanho do REDO precisa ser analisado, para reduzir o tamanho. Alto de espera para o SQL Id = 0mmz1cp8pvrnt 'begin wwv_flow.ajax(p_json=>:1 , p_instance=>:2 , p_flow_step_id=>:3 , p_flow_id=>:4 , p_context=>:5 , p_debug=>:6 ); commit; end;' teve 28 execuções e consumiu 92% do tempo total, consumiu 89% do CPU. SQL ID b39m8n96gxk7c tem muitas leituras fisicas

--12 11. Create a program (plsql and/or java, or any other language) that can extract to a flat file (csv), 1 file per location: the item, department unit cost, stock on hand quantity and stock value.
--Creating the 1000 files should take less than 30s.

-- funcao para o export
CREATE OR REPLACE PROCEDURE EMP_CSV AS
  CURSOR c_data IS
    SELECT loc,
	       item,
	       dept,
		   unit_cost,
		   stock_on_hand, 
		   stock_value
    FROM   item_loc_soh_hist
    ORDER BY loc;
    
  v_file  UTL_FILE.FILE_TYPE;
  separetor VARCHAR2(2) = ';';
  loc       number      = 0;
  loc_old   number      = 0;
BEGIN

  FOR cur_rec IN c_data LOOP
    loc = cur_rec.loc;
	
	if loc = 0 or loc != loc_old then 
	  if loc > 0 and loc_old > 0 then 
	    UTL_FILE.FCLOSE(v_file);
	  end if;
		
      v_file := UTL_FILE.FOPEN(location     => 'c:\',
                               filename     => loc ||'.csv',
                               open_mode    => 'w',
                               max_linesize => 32767);	
							   
      loc_old = loc;							   
    end if;
  
    UTL_FILE.PUT_LINE(v_file,
                      cur_rec.item           || separetor ||
                      cur_rec.dept           || separetor ||
                      TO_NUMBER(cur_rec.unit_cost,'9999.99')      || separetor ||
                      TO_NUMBER(cur_rec.stock_on_hand,'9999.99')  || separetor ||
                      TO_NUMBER(cur_rec.stock_value,'9999.99'));
  END LOOP;
  
  UTL_FILE.FCLOSE(v_file);
  
EXCEPTION
  WHEN OTHERS THEN
    UTL_FILE.FCLOSE(v_file);
    RAISE;
END;