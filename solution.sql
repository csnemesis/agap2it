--1 - Adicionando PK as tabelas e FK
ALTER table item
ADD CONSTRAINT pk_item PRIMARY KEY (item,dept);

ALTER table loc
ADD CONSTRAINT pk_loc PRIMARY KEY (loc);

ALTER table item_loc_soh
ADD CONSTRAINT pk_item_loc_soh PRIMARY KEY (item,loc,dept);


--2 - primeira sugestao seria por 100 registros, se nao tiver performance adicionar por loc e dept
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

--3 - Otimizar as consultas, validar se os indices que estão criados estao sendo usados corretamente, caso negativo alteralos e verificar se é necessario criar novos, verificar se existe partition para as tabelas que estao sendo usadas.

--4 - quais são os required fields? segue um exemplo
Create or replace view v_item_loc_soh as 
 select item, loc, dept, unit_cost, stock_on_hand
  from item_loc_soh;
  
--5 - seria melhor ter mais informações, tal como se ele pode acessar mais de um dept, para criar a tabela que atenda melhor a necessidade
create table user_dept (
    user_id varchar2(25) not null,
    dept number(4) not null
);

--6 - tabela para receber os registros
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

--7 - 

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
  
;
	
  end move_history;	


--8 - não fiz a criação de pipelines

--9 - A partida a tabela não foi PK e indices, sugestao seria criar pk e indices para a tabela
ALTER table item_loc_soh
ADD CONSTRAINT pk_item_loc_soh PRIMARY KEY (item,loc,dept);
CREATE INDEX EMP_NAME_IX ON EMPLOYEES (item,loc,dept);

--10 -  esta a dar o erro ao inserir os registros ORA-01536: space quota exceeded for tablespace 'APEX_BIGFILE_INSTANCE_TBS5' não consegui ter os total de dados

--11 - nao tenho conhecimento em AWR report 

--12 - funcao para o export
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
	  if loc > 0 then 
	    UTL_FILE.FCLOSE(v_file);
	  end if;
		
      v_file := UTL_FILE.FOPEN(location     => 'EXTRACT_DIR',
                               filename     => loc ||'.csv',
                               open_mode    => 'w',
                               max_linesize => 32767);	
							   
      loc_old = loc;							   
    end if;
  
    UTL_FILE.PUT_LINE(v_file,
                      cur_rec.item           || separetor ||
                      cur_rec.dept           || separetor ||
                      cur_rec.unit_cost      || separetor ||
                      cur_rec.stock_on_hand  || separetor ||
                      cur_rec.stock_value);
  END LOOP;
  
  UTL_FILE.FCLOSE(v_file);
  
EXCEPTION
  WHEN OTHERS THEN
    UTL_FILE.FCLOSE(v_file);
    RAISE;
END;