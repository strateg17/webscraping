"""
This file was created to store basic information about database formation and scripts from all table creation.
The data COLLACATION Cyrillic_General_CI_AS for normal work with Unicode.
"""

-- Base table to store every offer each day.
CREATE TABLE OLXparsing (
	execution_date DATE,
	offer_id VARCHAR(255),
	publication_date DATE,
	name NVARCHAR(255),
	price FLOAT,
	offer_info NVARCHAR(max),
	tag_seller NVARCHAR(255),
	tag_brand VARCHAR(255),
	tag_os VARCHAR(255),
	tag_screen VARCHAR(255),
	tag_condition NVARCHAR(255),
	seller_name NVARCHAR(255),
	seller_hist DATE,
	PRIMARY KEY (execution_date,offer_id)
);


CREATE TABLE OLXparsing_status (
	execution_date DATE , -- 
	offer_id VARCHAR(255), -- ID
	offer_url VARCHAR(MAX), -- to store urls
	offer_status INT -- active = 1 or not = 0
	PRIMARY KEY (execution_date, offer_id)
);


-- Find the rating of quantity of the records for each amount of records
WITH temp (offer_id, dates) 
AS
(
SELECT offer_id, COUNT(execution_date) as dates
FROM dbo.OLXparsing
GROUP BY offer_id
)

SELECT dates as number_of_records, COUNT(offer_id) as quantity
FROM temp
GROUP BY dates
ORDER BY dates DESC;


-- Find the not processed offer pages with today execution date
SELECT COUNT(DISTINCT offer_id)
FROM dbo.OLXparsing
WHERE offer_id NOT IN 
            (
                SELECT DISTINCT offer_id
                FROM dbo.OLXparsing
                WHERE execution_date = '2021-09-20'
            );

-- Find the not processed ID and URLs from the status SQL table
SELECT offer_id, offer_url
FROM dbo.OLXparsing_status
WHERE offer_status = 1
AND execution_date = '2021-09-22' -- today
AND offer_id NOT IN 
            (
                SELECT DISTINCT offer_id
                FROM dbo.OLXparsing
                WHERE execution_date = '2021-09-22'  -- today
            );

-- For testing purposes
INSERT INTO[dbo].[OLXparsing_status] VALUES 
('2021-09-21','468369414','https://www.olx.ua/d/uk/obyavlenie/iphone-7-rose-black-gold-32-128-256-z-garantyu-do-2-rokv-IDwHeho.html?sd=1#fc269cad9b',1),
('2021-09-21','469925080','https://www.olx.ua/d/uk/obyavlenie/iphone-7-silver-rose-32-128gb-garantya-do-2-rokv-magazin-iphonehouse-IDwNKYM.html?sd=1#faa19acf8d',1),
('2021-09-21','495975566','https://www.olx.ua/d/uk/obyavlenie/apple-iphone-7-32-gb-matte-black-otlichnoe-sostoyanie-est-vybor-IDxz3TU.html#17597a748d',1),
('2021-09-21','515900464','https://www.olx.ua/d/uk/obyavlenie/magazin-iphone-7-32gb-black-matt-gold-rosegold-neverlock-garantiya-IDyUFhe.html?sd=1#63d7baae75',1),
('2021-09-21','522794460','https://www.olx.ua/d/uk/obyavlenie/b-u-iphone-7-32gb-u-yabko-ternopl-ruska-19-valova-4-kredit-obmn-IDznAII.html#ccfcd98f98',1),

('2021-09-20','528925306','https://www.olx.ua/d/uk/obyavlenie/apple-iphone-7-32gb-neverlock-magazin-iroom-garantiya-obmen-IDzNjDk.html?sd=1#e5c632fa52',1),
('2021-09-20','537144652','https://www.olx.ua/d/uk/obyavlenie/iphone-7-32gb-neverlock-black-original-rassrochka-magazin-garantiya-IDAlNRq.html?sd=1#b1e386ea47;promoted',1),
('2021-09-20','543054226','https://www.olx.ua/d/uk/obyavlenie/aktsya-apple-iphone-7-32-ayfon-7-black-32-chorniy-mat-garantya-zevo-IDAKBd8.html?sd=1#ba1b5772f8',1),
('2021-09-20','545062430','https://www.olx.ua/d/uk/obyavlenie/7-7-black-rose-z-garantyu-do-2-rokv-magazin-iphone-house-IDAT1Dv.html?sd=1#122af838b5',1),
('2021-09-20','569298662','https://www.olx.ua/d/uk/obyavlenie/b-u-iphone-7-32-128-gb-kredit-obmn-v-yabko-bts-rus-IDCvIAu.html#3445f0e2d5;promoted',1),

('2021-09-19','720351035','https://www.olx.ua/d/uk/obyavlenie/iphone-8-neverlock-IDMKvd5.html',1);

