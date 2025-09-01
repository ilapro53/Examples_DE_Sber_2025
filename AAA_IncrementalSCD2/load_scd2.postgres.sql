----------------------------------------------------
-- Подготовка хранилища (SCD2)
----------------------------------------------------

DROP TABLE IF EXISTS detn.ilka_source;
DROP TABLE IF EXISTS detn.ilka_stg;
DROP TABLE IF EXISTS detn.ilka_stg_del;
DROP TABLE IF EXISTS detn.ilka_target_hist;
DROP TABLE IF EXISTS detn.ilka_meta;

-- Источник
create table detn.ilka_source ( 
	id integer,
	val varchar(50),
	update_dt timestamp(0) -- Дата обновления в источнике
);

-- Staging
create table detn.ilka_stg ( 
	id integer,
	val varchar(50),
	update_dt timestamp(0), -- Дата обновления в источнике
	processed_dt timestamp(0) -- Дата загрузуи в Staging
);

-- Не удаленные записи Staging
create table detn.ilka_stg_del ( 
	id integer
);

-- Целевая таблица SCD2
create table detn.ilka_target_hist (
	id integer,
	val varchar(50),
	effective_from timestamp(0), -- Начало периода
	effective_to timestamp(0), -- Конец периода
	deleted_flg char(1) NOT NULL DEFAULT 'N' CHECK (deleted_flg IN ('Y', 'N'))
);

-- Мета таблица
create table detn.ilka_meta (
    schema_name varchar(30),
    table_name varchar(30),
    max_update_dt timestamp(0)
);

-- Установка первичных значений мета таблицы
insert into detn.ilka_meta( schema_name, table_name, max_update_dt )
values( 'detn', 'ilka_source', to_timestamp('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS') );


----------------------------------------------------
-- Инкрементальная загрузка (SCD2)
----------------------------------------------------



------ [ Работа со Staging (stg и stg_del) ] ------ 

-- 1. Очистка staging
delete from detn.ilka_stg;
delete from detn.ilka_stg_del;

-- 2. Захват данных из источника в staging
insert into detn.ilka_stg (id, val, update_dt, processed_dt)
SELECT distinct ON (id, val, update_dt) id, val, update_dt, now() from detn.ilka_source
where update_dt > ( -- Фильтр по последней обработанной дате
	select max_update_dt
	from detn.ilka_meta
	where schema_name = 'detn' and table_name = 'ilka_source'
);

-- Фиксация удаления данных в staging (stg_del)
insert into detn.ilka_stg_del (id)
select id from detn.ilka_source;


-- 3. Применение данных в приемник DDS (вставка)
insert into detn.ilka_target_hist (id, val, effective_from, effective_to, deleted_flg)
select
	stg.id, -- id
	stg.val, -- значение
	stg.update_dt, -- effective_from -- активно с момента добавления в источнике
	to_timestamp('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS'), -- effective_to -- актино до условной бесконечности (текущий момент)
	'N' -- не удалено
from detn.ilka_stg stg
left join ( -- Выявляем отсутствующие в Target (таблице фактов) записи
		-- действительные записи (таблица фактов Target)
	    SELECT * FROM detn.ilka_target_hist 
	    WHERE 
	    	effective_to = to_timestamp('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') -- актино до условной бесконечности (текущий момент)
	    	AND deleted_flg = 'N' -- не удалено
	) tgt
	on stg.id = tgt.id
left join ( -- Выявляем отсутствующие даленных из Target (таблице фактов удаленных из Target) записи
		-- недействительные в текущий момент записи (таблица фактов удаленных из Target)
	    SELECT * FROM detn.ilka_target_hist 
	    WHERE 
	    	effective_to = to_timestamp('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') -- актино до условной бесконечности (текущий момент)
	    	AND deleted_flg = 'Y' -- удалено
	) del
	on stg.id = del.id
WHERE
	tgt.id is NULL AND -- Только отсутствующие в target записи
	del.id IS NULL; -- Только отсутствующие в удаленных из Target записи


-- 4.1 Применение данных в приемник DDS (обновление): закрытие записей
update detn.ilka_target_hist upd_tgt
set effective_to = stg.update_dt - interval '1 SECOND' -- старая запись дейтвует до начала новой (т.е. обновления) минус интервал
from detn.ilka_stg stg
inner join ( -- только перечечения Staging и Target (таблицы фактов)
		-- действительные записи (таблица фактов Target)
	    SELECT * FROM detn.ilka_target_hist 
	    WHERE 
	    	effective_to = to_timestamp('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') -- актино до условной бесконечности (текущий момент)
	    	AND deleted_flg = 'N' -- не удалено
	) tgt
    on stg.id = tgt.id
where 
    upd_tgt.id = tgt.id -- указываем связь обновляемой таблицы и полученного перечечения (Staging и Target (таблицы фактов))
	and upd_tgt.effective_to = to_timestamp('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')  -- актино до условной бесконечности (текущий момент) (еще раз для точности)
	and tgt.deleted_flg = 'N' -- не обрабатываем уже удаленные данные
    and stg.update_dt > tgt.effective_from -- только данные, появившиеся полсе текущей актуальной записи
    and ( -- только измененные
        stg.val != tgt.val -- Обнаруживаем изменения
        or (stg.val is null and tgt.val is not null) -- Обнаруживаем изменения НА null
        or (stg.val is not null and tgt.val is null) -- Обнаруживаем изменения null
    );


-- 4.2 Применение данных в приемник DDS (обновление): вставка записей
insert into detn.ilka_target_hist (id, val, effective_from, effective_to, deleted_flg)
SELECT DISTINCT -- берем изменения из Staging
	stg.id AS id, -- id
	stg.val AS val, -- значение
	stg.update_dt AS effective_from, -- начинает действовать с момента обновления в источнике
	to_timestamp('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS effective_to, -- дествует до условной бескончности
	'N' AS deleted_flg -- не удалено
from detn.ilka_stg stg
inner join ( -- только перечесение последних закрытых записей и staging 
		-- таблица последних закрытых записей (должны были быть закрыты в предыдущем запросе)
	    SELECT 
	        h2.id,
	        h2.val,
	        h2.effective_from,
	        h2.effective_to
		FROM (
			SELECT -- дата окончания самой актуальной записи или самой последней из неактуальных для всех id
				ilka_target_hist.id, 
				MAX(effective_to) AS max_effective_to 
			FROM detn.ilka_target_hist 
		    GROUP BY ilka_target_hist.id
	    ) h1
	    LEFT JOIN detn.ilka_target_hist h2
	    	ON -- возвращаем таблице ее же поля. Соежиняем по дате для каждого id
	    		h1.id = h2.id 
	    		AND h1.max_effective_to = h2.effective_to
	    WHERE
	    	effective_to <> to_timestamp('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') -- отбрасываем действующие записи
	    	AND deleted_flg = 'N' -- отбрасываем удаленные объекты
	) closed
on stg.id = closed.id
where -- берем записи где
	stg.update_dt > closed.effective_from -- новые данные появились позже начала последней закрытой записи
	and ( -- новые данные отличаются от последней закрытой записи
        stg.val != closed.val -- Обнаруживаем изменения
        or (stg.val is null and closed.val is not null) -- Обнаруживаем изменения НА null
        or (stg.val is not null and closed.val is null) -- Обнаруживаем изменения null
    );


-- 5.1 Применение данных в приемник DDS (удаление): закрытие записей
update detn.ilka_target_hist tgt
set effective_to = NOW() - interval '1 SECOND' -- закрываем запись текущем временем минус интервал
where 
	tgt.deleted_flg = 'N' -- не обрабатываем уже удаленные данные
	and tgt.effective_to = to_timestamp('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS')  -- актино до условной бесконечности (текущий момент)
	and tgt.deleted_flg = 'N' -- не обрабатываем уже удаленные данные
	AND tgt.id IN ( -- выбрать ID отсутствующих в stg_del записей
		select
			tgt2.id
		from (
			-- действительные записи (таблица фактов Target)
		    SELECT * FROM detn.ilka_target_hist 
		    WHERE 
		    	effective_to = to_timestamp('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') -- актино до условной бесконечности (текущий момент)
		    	AND deleted_flg = 'N' -- не удалено
		) tgt2
		left join detn.ilka_stg_del stg -- к target присоединить stg_del
			on stg.id = tgt2.id
		where stg.id is NULL -- ID есть в target, но отсутствует в stg_del
	);


-- 5.2 Применение данных в приемник DDS (удаление): вставка записей
insert into detn.ilka_target_hist (id, val, effective_from, effective_to, deleted_flg)
select
	closed.id AS id, -- забираем id из предыдущей записи
	closed.val AS val, -- забираем значение из предыдущей записи
	closed.effective_to + interval '1 SECOND' AS effective_from, -- действует с момента закрытия предыдущей записи + интервал (закрыли на предыдущем шаге)
	to_timestamp('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') AS effective_to, -- effective_to бескончность (актуальна сейчас)
	'Y' AS deleted_flg -- устанавливаем флаг удаления
FROM ( -- берем данные недавно закрытых (закрыли на предыдущем шаге) удаленных записей
		-- таблица последних закрытых записей
	    SELECT 
	        h2.id,
	        h2.val,
	        h2.effective_from,
	        h2.effective_to
		FROM (
			SELECT -- дата окончания самой актуальной записи или самой последней из неактуальных для всех id
				ilka_target_hist.id, 
				MAX(effective_to) AS max_effective_to 
			FROM detn.ilka_target_hist 
		    GROUP BY ilka_target_hist.id
	    ) h1
	    LEFT JOIN detn.ilka_target_hist h2
	    	ON -- возвращаем таблице ее же поля. Соежиняем по дате для каждого id
	    		h1.id = h2.id 
	    		AND h1.max_effective_to = h2.effective_to
	    WHERE
	    	effective_to <> to_timestamp('9999-12-31 23:59:59', 'YYYY-MM-DD HH24:MI:SS') -- отбрасываем действующие записи
	    	AND deleted_flg = 'N' -- отбрасываем уже удаленные объекты
	) closed
left join detn.ilka_stg_del stg_del -- к последним закрытым записям присоединить stg_del
	on stg_del.id = closed.id
where stg_del.id is NULL; -- ID есть в последних закрытых записях, но отсутствует в stg_del


-- 6. Сохраняем состояние загрузки в метаданные.
update detn.ilka_meta
set -- обновим максимальную существующую дату
	max_update_dt = coalesce(
		(select max(update_dt) from detn.ilka_stg), -- дата самого поздего обновления в Staging, если она есть
			(
				SELECT GREATEST( -- елси нет даты, то оставляем текущую или ставим условную бесконечность, если текущей нет
				    (SELECT max_update_dt FROM detn.ilka_meta),
				    to_timestamp('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')
		    )
		) 
	)
where 
	1=1
	AND schema_name = 'detn' -- фильтруем поле схемы
	AND table_name = 'ilka_source';  -- фильтруем поле имени таблицы


-- 7. Фиксация транзакции
commit;
