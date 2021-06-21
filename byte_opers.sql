drop schema if exists export_tools cascade;
create schema export_tools;

-- drop function if exists export_tools.form_empty_bytea(str_length int4);
drop function if exists export_tools.form_empty_bytea(str_length int4, default_value bytea);
create or replace function export_tools.form_empty_bytea(str_length int4, default_value bytea default null::bytea) returns bytea as $$
    declare bstr bytea;
    begin
        select string_agg(coalesce(default_value, E'\\000'::bytea), '') into bstr from generate_series(1, str_length) i;
        return bstr;
    end;
$$ language plpgsql;

drop function if exists export_tools.bytea_to_bit_varying_arr(bstr bytea);
create or replace function export_tools.bytea_to_bit_varying_arr(bstr bytea) returns bit varying [] as $$
    declare str text;
    begin
    select array_agg((c.ch) order by i.index) into str
    from (select bstr) s
    cross join length(s.bstr) l(length)
    cross join generate_series(0, l.length - 1) i(index)
    cross join lateral (select get_byte(s.bstr, i.index)::bit(8):: bit varying(8)) c(ch);
    return str;
    end;
$$ language plpgsql;


drop function if exists export_tools.text_to_bytea(str text);
create or replace function export_tools.text_to_bytea(str text) returns bytea as $$ begin return decode(str, 'escape'); end; $$ language plpgsql;

drop function if exists export_tools.bytea_to_text(str text);
create or replace function export_tools.bytea_to_text(bstr bytea) returns text as $$ begin return encode(bstr, 'escape'); end; $$ language plpgsql;

drop function if exists export_tools.json_bit_varying_arr_to_bytea(arr bit varying[]);
create or replace function export_tools.json_bit_varying_arr_to_bytea(arr bit varying []) returns bytea as $$
    declare res bytea;
    declare new_length int4;
    declare buffer_byte int;
    declare byte_index int4;
    declare bit_index int4;
    declare bit_curr int4;
    declare bit_array int4[];
    begin

        bit_array = (select array_agg(b.bit order by i.index, l.index) from unnest(arr) with ordinality i(el, index) cross join generate_series(0, length(i.el::bit varying) - 1) l(index) cross join get_bit(i.el, l.index) b(bit));
        new_length = ceil((select sum(length(a.el)) from unnest(arr) a(el)) / 8.0);
        res = export_tools.form_empty_bytea(new_length);

        bit_curr = 1;
        byte_index = 0;
        bit_index = 7;
        buffer_byte = B'0000000'::bit(8)::int;
        while byte_index < new_length loop
               buffer_byte = buffer_byte << 1 | coalesce(bit_array[bit_curr], 0);
            bit_curr = bit_curr + 1;

            if bit_index = 0 or bit_curr > new_length * 8 then
                res = set_byte(res, byte_index, buffer_byte);
                bit_index = 7;
                buffer_byte = B'00000000';
                byte_index = byte_index + 1;
            else
                bit_index = bit_index - 1;
            end if;

        end loop;
        return res;
    end;
    $$ language plpgsql;

drop type if exists export_tools.qr_correction_level_type cascade;
create type export_tools.qr_correction_level_type as enum('L', 'M', 'Q', 'H');
drop table if exists export_tools.qr_settings cascade;
create table export_tools.qr_settings (
    qr_settingsid serial primary key,
    qr_version int4,
    qr_correction_level export_tools.qr_correction_level_type,
    byte_length int4,
    block_count int4,
    correction_byte_count int4
);
alter table export_tools.qr_settings add column bit_data_length int4;
alter table export_tools.qr_settings add column alignment_positions _int4;
alter table export_tools.qr_settings add column code_version bit varying(6)[];
alter table export_tools.qr_settings add column bits_length int4;

select * from export_tools.qr_settings;

drop table if exists export_tools.qr_gen_array;
create table export_tools.qr_gen_array(correction_byte_count int4, gen_array bit varying(8)[]);
insert into export_tools.qr_gen_array
select v.byte_count, v.gen_arr::bit(8)[]::bit varying(8)[]
from (values(7, array[87, 229, 146, 149, 238, 102, 21]),
(10, array[251, 67, 46, 61, 118, 70, 64, 94, 32, 45]),
(13, array[74, 152, 176, 100, 86, 100, 106, 104, 130, 218, 206, 140, 78]),
(15, array[8, 183, 61, 91, 202, 37, 51, 58, 58, 237, 140, 124, 5, 99, 105]),
(16, array[120, 104, 107, 109, 102, 161, 76, 3, 91, 191, 147, 169, 182, 194, 225, 120]),
(17, array[43, 139, 206, 78, 43, 239, 123, 206, 214, 147, 24, 99, 150, 39, 243, 163, 136]),
(18, array[215, 234, 158, 94, 184, 97, 118, 170, 79, 187, 152, 148, 252, 179, 5, 98, 96, 153]),
(20, array[17, 60, 79, 50, 61, 163, 26, 187, 202, 180, 221, 225, 83, 239, 156, 164, 212, 212, 188, 190]),
(22, array[210, 171, 247, 242, 93, 230, 14, 109, 221, 53, 200, 74, 8, 172, 98, 80, 219, 134, 160, 105, 165, 231]),
(24, array[229, 121, 135, 48, 211, 117, 251, 126, 159, 180, 169, 152, 192, 226, 228, 218, 111, 0, 117, 232, 87, 96, 227, 21]),
(26, array[173, 125, 158, 2, 103, 182, 118, 17, 145, 201, 111, 28, 165, 53, 161, 21, 245, 142, 13, 102, 48, 227, 153, 145, 218, 70]),
(28, array[168, 223, 200, 104, 224, 234, 108, 180, 110, 190, 195, 147, 205, 27, 232, 201, 21, 43, 245, 87, 42, 195, 212, 119, 242, 37, 9, 123]),
(30, array[41, 173, 145, 152, 216, 31, 179, 182, 50, 48, 110, 86, 239, 96, 222, 125, 42, 173, 226, 193, 224, 130, 156, 37, 251, 216, 238, 40, 192, 180])) v(byte_count, gen_arr);
select * from export_tools.qr_gen_array;

with version_table as (
    select *
        from (values (1, 152, 128, 104, 72),
                     (2, 272, 224, 176, 128),
                     (3, 440, 352, 272, 384),
                     (4, 640, 512, 384, 288),
                     (5, 864, 688, 496, 368),
                     -- 5
                     (6, 1088, 964, 608, 480),
                     (7, 1248, 992, 704, 528),
                     (8, 1552, 1232, 880, 688),
                     (9, 1856, 1456, 1056, 800),
                     (10, 2192, 1728, 1232, 976),
                     -- 10
                     (11, 2592, 2032, 1440, 1120),
                     (12, 2960, 2320, 1648, 1264),
                     (13, 3424, 2672, 1952, 1440),
                     (14, 3688, 2920, 2088, 1576),
                     (15, 4184, 3320, 2360, 1784),
                     -- 15
                     (16, 4712, 3624, 2600, 2024),
                     (17, 5168, 4056, 2936, 2264),
                     (18, 5768, 4504, 3176, 2504),
                     (19, 6360, 5016, 3560, 2728),
                     (20, 6888, 5352, 3880, 3080),
                     -- 20
                     (21, 7456, 5712, 4096, 3248),
                     (22, 8048, 6256, 4544, 3536),
                     (23, 8752, 6880, 4912, 3712),
                     (24, 9392, 7312, 5312, 4112),
                     (25, 10208, 8000, 5744, 4304),
                     -- 25
                     (26, 10960, 8496, 6032, 4768),
                     (27, 11744, 9024, 6464, 5024),
                     (28, 12248, 9544, 6968, 5288),
                     (29, 13048, 10136, 7288, 5608),
                     (30, 13880, 10984, 7880, 5960),
                     -- 30
                     (31, 14744, 11640, 8264, 6344),
                     (32, 15640, 12328, 8920, 6760),
                     (33, 16568, 13048, 9368, 7208),
                     (34, 17528, 13800, 9848, 7688),
                     (35, 18448, 14496, 10288, 7888),
                     -- 35
                     (36, 19472, 15312, 10832, 8432),
                     (37, 20528, 15936, 11408, 8768),
                     (38, 21616, 16816, 12016, 9136),
                     (39, 22496, 17728, 12656, 9776),
                     (40, 23648, 18672, 13328, 10208)
                 -- 40
             ) v(version, l, m, q, h)
),
all_length as (
    select v.version, 'L'::export_tools.qr_correction_level_type as correction_level, v.l as bits_length from version_table v
    union
    select v.version, 'M'::export_tools.qr_correction_level_type as correction_level, v.m as bits_length  from version_table v
    union
    select v.version, 'Q'::export_tools.qr_correction_level_type as correction_level, v.q as bits_length  from version_table v
    union
    select v.version, 'H'::export_tools.qr_correction_level_type as correction_level, v.h as bits_length  from version_table v
)
-- update export_tools.qr_settings s set bits_length = p.bits_length
-- from all_length p where s.qr_version = p.version and s.qr_correction_level = p.correction_level;
insert into export_tools.qr_settings (qr_version, qr_correction_level, bits_length)
select l.version, l.correction_level, l.bits_length from all_length l;

select * from export_tools.qr_settings;

with version_table as (
    select *
        from (values (1, 17, 14, 11, 7),
                     (2, 32, 26, 20, 14),
                     (3, 53, 42, 32, 24),
                     (4, 78, 62, 46, 34),
                     (5, 106, 84, 60, 44),
                     -- 5
                     (6, 134, 106, 74, 58),
                     (7, 154, 122, 86, 66),
                     (8, 192, 152, 108, 84),
                     (9, 230, 180, 130, 98),
                     (10, 271, 213, 151, 119),
                     -- 10
                     (11, 321, 251, 177, 137),
                     (12, 367, 287, 203, 155),
                     (13, 425, 331, 241, 177),
                     (14, 458, 362, 258, 194),
                     (15, 520, 412, 292, 220),
                     -- 15
                     (16, 586, 450, 322, 250),
                     (17, 644, 504, 364, 280),
                     (18, 718, 560, 394, 310),
                     (19, 792, 624, 442, 338),
                     (20, 858, 666, 482, 382),
                     -- 20
                     (21, 929, 711, 509, 403),
                     (22, 1003, 779, 565, 439),
                     (23, 1091, 857, 611, 461),
                     (24, 1171, 911, 661, 511),
                     (25, 1273, 997, 715, 535),
                     -- 25
                     (26, 1367, 1059, 751, 593),
                     (27, 1465, 1125, 805, 625),
                     (28, 1528, 1190, 868, 658),
                     (29, 1628, 1264, 908, 698),
                     (30, 1732, 1370, 982, 742),
                     -- 30
                     (31, 1840, 1452, 1030, 790),
                     (32, 1952, 1538, 1112, 842),
                     (33, 2068, 1628, 1168, 898),
                     (34, 2188, 1722, 1228, 958),
                     (35, 2303, 1809, 1283, 983),
                     -- 35
                     (36, 2431, 1911, 1351, 1051),
                     (37, 2563, 1989, 1423, 1093),
                     (38, 2699, 2099, 1499, 1139),
                     (39, 2809, 2213, 1579, 1219),
                     (40, 2953, 2331, 1663, 1273)
                 -- 40
             ) v(version, l, m, q, h)
),
all_length as (
    select v.version, 'L'::export_tools.qr_correction_level_type as correction_level, v.l as byte_length  from version_table v
    union
    select v.version, 'M'::export_tools.qr_correction_level_type as correction_level, v.m as byte_length  from version_table v
    union
    select v.version, 'Q'::export_tools.qr_correction_level_type as correction_level, v.q as byte_length  from version_table v
    union
    select v.version, 'H'::export_tools.qr_correction_level_type as correction_level, v.h as byte_length  from version_table v
)
update export_tools.qr_settings s set byte_length = p.byte_length
from all_length p where s.qr_version = p.version and s.qr_correction_level = p.correction_level;

update export_tools.qr_settings set byte_length = bits_length / 8;

update export_tools.qr_settings s set bit_data_length = case when s.qr_version < 10 then 8 else 16 end;
select * from export_tools.qr_settings;

with version_table as (
    select * from (values (1, 1, 1, 1, 1),
                     (2, 1, 1, 1, 1),
                     (3, 1, 1, 2, 2),
                     (4, 1, 2, 2, 4),
                     (5, 1, 2, 4, 4),
                     -- 5
                     (6, 2, 4, 4, 4),
                     (7, 2, 4, 6, 5),
                     (8, 2, 4, 6, 6),
                     (9, 2, 5, 8, 8),
                     (10, 4, 5, 8, 8),
                     -- 10
                     (11, 4, 5, 8, 11),
                     (12, 4, 8, 10, 11),
                     (13, 4, 9, 12, 16),
                     (14, 4, 9, 16, 16),
                     (15, 6, 10, 12, 18),
                     -- 15
                     (16, 6, 10, 17, 16),
                     (17, 6, 11, 16, 19),
                     (18, 6, 13, 18, 21),
                     (19, 7, 14, 21, 25),
                     (20, 8, 16, 20, 25),
                     -- 20
                     (21, 8, 17, 23, 25),
                     (22, 9, 17, 23, 34),
                     (23, 9, 18, 25, 30),
                     (24, 10, 20, 27, 32),
                     (25, 12, 21, 29, 35),
                     -- 25
                     (26, 12, 23, 34, 37),
                     (27, 12, 25, 34, 40),
                     (28, 13, 26, 35, 42),
                     (29, 14, 28, 38, 45),
                     (30, 15, 29, 40, 48),
                     -- 30
                     (31, 16, 31, 43, 51),
                     (32, 17, 33, 45, 54),
                     (33, 18, 35, 48, 57),
                     (34, 19, 37, 51, 60),
                     (35, 19, 38, 53, 63),
                     -- 35
                     (36, 20, 40, 56, 66),
                     (37, 21, 43, 59, 70),
                     (38, 22, 45, 62, 74),
                     (39, 24, 47, 65, 77),
                     (40, 25, 49, 68, 81)
                     -- 40
        ) v(version, l, m, q, h)
),
all_length as (
    select v.version, 'L'::export_tools.qr_correction_level_type as correction_level, v.l as value  from version_table v
    union
    select v.version, 'M'::export_tools.qr_correction_level_type as correction_level, v.m as value  from version_table v
    union
    select v.version, 'Q'::export_tools.qr_correction_level_type as correction_level, v.q as value  from version_table v
    union
    select v.version, 'H'::export_tools.qr_correction_level_type as correction_level, v.h as value  from version_table v
)
update export_tools.qr_settings s set block_count = p.value
from all_length p where s.qr_version = p.version and s.qr_correction_level = p.correction_level;

select * from export_tools.qr_settings;

with version_table as (
    select * from (values (1, 7,10,13,17),
                (2, 10,16,22,28),
                (3, 15,26,18,22),
                (4, 20,18,26,16),
                (5, 26,24,18,22),
                (6, 18,16,24,28),
                (7, 20,18,18,26),
                (8, 24,22,22,26),
                (9, 30,22,20,24),
                (10, 18,26,24,28),
                (11, 20,30,28,24),
                (12, 24,22,26,28),
                (13, 26,22,24,22),
                (14, 30,24,20,24),
                (15, 22,24,30,24),
                (16, 24,28,24,30),
                (17, 28,28,28,28),
                (18, 30,26,28,28),
                (19, 28,26,26,26),
                (20, 28,26,30,28),
                (21, 28,26,28,30),
                (22, 28,28,30,24),
                (23, 30,28,30,30),
                (24, 30,28,30,30),
                (25, 26,28,30,30),
                (26, 28,28,28,30),
                (27, 30,28,30,30),
                (28, 30,28,30,30),
                (29, 30,28,30,30),
                (30, 30,28,30,30),
                (31, 30,28,30,30),
                (32, 30,28,30,30),
                (33, 30,28,30,30),
                (34, 30,28,30,30),
                (35, 30,28,30,30),
                (36, 30,28,30,30),
                (37, 30,28,30,30),
                (38, 30,28,30,30),
                (39, 30,28,30,30),
                (40, 30,28,30,30)
        ) v(version, l, m, q, h)
),
all_length as (
    select v.version, 'L'::export_tools.qr_correction_level_type as correction_level, v.l as value  from version_table v
    union
    select v.version, 'M'::export_tools.qr_correction_level_type as correction_level, v.m as value  from version_table v
    union
    select v.version, 'Q'::export_tools.qr_correction_level_type as correction_level, v.q as value  from version_table v
    union
    select v.version, 'H'::export_tools.qr_correction_level_type as correction_level, v.h as value  from version_table v
)
update export_tools.qr_settings s set correction_byte_count = p.value
from all_length p where s.qr_version = p.version and s.qr_correction_level = p.correction_level;

with version_table as (
    select * from (values (1,array[]::_int4),
            (2,array[18]::_int4),
            (3,array[22]::_int4),
            (4,array[26]::_int4),
            (5,array[30]::_int4),
            (6,array[34]::_int4),
            (7,array[6, 22, 38]::_int4),
            (8,array[6, 24, 42]::_int4),
            (9,array[6, 26, 46]::_int4),
            (10,array[6, 28, 50]::_int4),
            (11,array[6, 30, 54]::_int4),
            (12,array[6, 32, 58]::_int4),
            (13,array[6, 34, 62]::_int4),
            (14,array[6, 26, 46, 66]::_int4),
            (15,array[6, 26, 48, 70]::_int4),
            (16,array[6, 26, 50, 74]::_int4),
            (17,array[6, 30, 54, 78]::_int4),
            (18,array[6, 30, 56, 82]::_int4),
            (19,array[6, 30, 58, 86]::_int4),
            (20,array[6, 34, 62, 90]::_int4),
            (21,array[6, 28, 50, 72, 94]::_int4),
            (22,array[6, 26, 50, 74, 98]::_int4),
            (23,array[6, 30, 54, 78, 102]::_int4),
            (24,array[6, 28, 54, 80, 106]::_int4),
            (25,array[6, 32, 58, 84, 110]::_int4),
            (26,array[6, 30, 58, 86, 114]::_int4),
            (27,array[6, 34, 62, 90, 118]::_int4),
            (28,array[6, 26, 50, 74, 98, 122]::_int4),
            (29,array[6, 30, 54, 78, 102, 126]::_int4),
            (30,array[6, 26, 52, 78, 104, 130]::_int4),
            (31,array[6, 30, 56, 82, 108, 134]::_int4),
            (32,array[6, 34, 60, 86, 112, 138]::_int4),
            (33,array[6, 30, 58, 86, 114, 142]::_int4),
            (34,array[6, 34, 62, 90, 118, 146]::_int4),
            (35,array[6, 30, 54, 78, 102, 126, 150]::_int4),
            (36,array[6, 24, 50, 76, 102, 128, 154]::_int4),
            (37,array[6, 28, 54, 80, 106, 132, 158]::_int4),
            (38,array[6, 32, 58, 84, 110, 136, 162]::_int4),
            (39,array[6, 26, 54, 82, 110, 138, 166]::_int4),
            (40,array[6, 30, 58, 86, 114, 142, 170]::_int4)
        ) v(version, value)
),
all_length as (
    select v.version, v.value  from version_table v
)
update export_tools.qr_settings s set alignment_positions = p.value
from all_length p where s.qr_version = p.version;

with version_table as (
    select * from (values (7, array[B'000010',B'011110',B'100110']::bit varying(6)[]),
        (8, array[B'010001',B'011100',B'111000']::bit varying(6)[]),
        (9, array[B'110111',B'011000',B'000100']::bit varying(6)[]),
        (10, array[B'101001',B'111110',B'000000']::bit varying(6)[]),
        (11, array[B'001111',B'111010',B'111100']::bit varying(6)[]),
        (12, array[B'001101',B'100100',B'011010']::bit varying(6)[]),
        (13, array[B'101011',B'100000',B'100000']::bit varying(6)[]),
        (14, array[B'110101',B'000110',B'100000']::bit varying(6)[]),
        (15, array[B'010011',B'000010',B'011100']::bit varying(6)[]),
        (16, array[B'011100',B'010001',B'011100']::bit varying(6)[]),
        (17, array[B'111010',B'010101',B'100000']::bit varying(6)[]),
        (18, array[B'100100',B'110011',B'100000']::bit varying(6)[]),
        (19, array[B'000010',B'110111',B'011000']::bit varying(6)[]),
        (20, array[B'000000',B'101001',B'111110']::bit varying(6)[]),
        (21, array[B'100110',B'101101',B'000000']::bit varying(6)[]),
        (22, array[B'111000',B'001011',B'000000']::bit varying(6)[]),
        (23, array[B'011110',B'001111',B'111000']::bit varying(6)[]),
        (24, array[B'001101',B'001101',B'100100']::bit varying(6)[]),
        (25, array[B'101011',B'001001',B'011000']::bit varying(6)[]),
        (26, array[B'110101',B'101111',B'011000']::bit varying(6)[]),
        (27, array[B'010011',B'101011',B'100000']::bit varying(6)[]),
        (28, array[B'010001',B'110101',B'000100']::bit varying(6)[]),
        (29, array[B'110111',B'110001',B'111000']::bit varying(6)[]),
        (30, array[B'101001',B'010111',B'111000']::bit varying(6)[]),
        (31, array[B'001111',B'010011',B'000010']::bit varying(6)[]),
        (32, array[B'101000',B'011000',B'101000']::bit varying(6)[]),
        (33, array[B'001110',B'011100',B'010000']::bit varying(6)[]),
        (34, array[B'010000',B'111010',B'010100']::bit varying(6)[]),
        (35, array[B'110110',B'111110',B'101000']::bit varying(6)[]),
        (36, array[B'110100',B'100000',B'001000']::bit varying(6)[]),
        (37, array[B'010010',B'100100',B'110000']::bit varying(6)[]),
        (38, array[B'001100',B'000010',B'110110']::bit varying(6)[]),
        (39, array[B'101010',B'000110',B'001000']::bit varying(6)[]),
        (40, array[B'111001',B'000100',B'010000']::bit varying(6)[])
        ) v(version, value)
),
all_length as (
    select v.version, v.value  from version_table v
)
update export_tools.qr_settings s set code_version = p.value
from all_length p where s.qr_version = p.version;

drop function if exists export_tools.get_qr_version(_byte_length int4, _qr_correction_level export_tools.qr_correction_level_type);
create or replace function export_tools.get_qr_version(_byte_length int4,
                                                       _qr_correction_level export_tools.qr_correction_level_type) returns int4 as
$$
declare
    _version int4;
begin
    with version_table as (
        select v.qr_version as version, v.byte_length as max_length
        from  export_tools.qr_settings v where v.qr_correction_level = _qr_correction_level
    )
    select min(v.version) into _version from version_table v where v.max_length >= _byte_length;
    return _version;
end;
$$ language plpgsql;

drop function if exists export_tools.qr_size(qr_version int4);
create or replace function export_tools.qr_size(qr_version int4) returns int4 as $$
    begin
        return case when qr_version > 0 and qr_version <= 40 then (qr_version * 4) + 17 else 0 end;
    end;
    $$ language plpgsql;

drop function if exists export_tools.qr_insert_pic_to_array(qr_array int[][], x int4, y int4, pic int[][], replace_default boolean);
create or replace function export_tools.qr_insert_pic_to_array(qr_array int[][], x int4, y int4, pic int[][], replace_default boolean default false) returns int[][] as $$
    declare curr_x int4;
    declare curr_y int4;

    declare size_x int4;
    declare size_y int4;

    declare has_cross_with_default_cells boolean = false;
    declare has_current_cell_cross boolean;

    declare temp_qr_array int[][];
    begin
        x = x - 1;
        y = y - 1;
        temp_qr_array = qr_array;

        size_y = array_length(qr_array, 1);
        size_x = array_length(qr_array, 2);

        for curr_y in (select generate_subscripts(pic, 1)) loop
            for curr_x in (select generate_subscripts(pic, 2)) loop
--                 raise notice 'qr_insert_pic_to_array % %, %, %, %, %, %, %, %', qr_array[curr_y + y][curr_x + x] = pic[curr_y][curr_x], curr_y, curr_x, curr_y + y, curr_x + x, qr_array[curr_y + y][curr_x + x],  pic[curr_y][curr_x], pic, qr_array;

                if size_y >= curr_y + y and size_x >= curr_x + x then
                    has_current_cell_cross = export_tools.qr_is_editable_cell(temp_qr_array[curr_y + y][curr_x + x]);
                    has_cross_with_default_cells = has_cross_with_default_cells or not has_current_cell_cross;
                    if (replace_default or has_current_cell_cross) then
                        temp_qr_array[curr_y + y][curr_x + x] = pic[curr_y][curr_x];
                    end if;
                end if;
            end loop;
        end loop;
    return case when replace_default or not has_cross_with_default_cells then temp_qr_array else qr_array end;
    end;
    $$ language plpgsql;

drop function if exists export_tools.qr_form_array(size_x int4, size_y int4, default_value int);
create or replace function export_tools.qr_form_array(size_x int4, size_y int4, default_value int default null::int) returns int[][] as $$
    begin
        return (select array_agg((select array_agg(default_value) from generate_series(1, size_x) g)) from generate_series(1, size_y) g1);
    end;
    $$ language plpgsql;

select export_tools.qr_form_array(1, 2, 3);

drop function if exists export_tools.qr_transpose_array(arr int[][]);
create or replace function export_tools.qr_transpose_array(arr int[][]) returns int[][] as $$
    declare new_arr int[][];
    declare curr_y int4;
    declare curr_x int4;
    declare temp_arr int[];
    begin
        new_arr = array[]::int[][];
        for curr_x in (select generate_subscripts(arr, 2)) loop
            temp_arr = array[]::int[];
            for curr_y in (select generate_subscripts(arr, 1)) loop
                temp_arr = array_append(temp_arr, arr[curr_y][curr_x]);
            end loop;
            new_arr = new_arr || array[temp_arr];
        end loop;
        return new_arr;
    end
    $$ language plpgsql;

drop function if exists export_tools.varying_arr_to_int_arr(bit_arr bit varying[], add_value int);
create or replace function export_tools.varying_arr_to_int_arr(bit_arr bit varying[], add_value int default 0) returns int[] as $$
    begin
       return (select array_agg((i.ch::int) | add_value order by a.index, i.index) from unnest(bit_arr) with ordinality a(el, index) cross join lateral unnest(regexp_split_to_array(a.el::text, '')) with ordinality i(ch, index));
    end;
    $$ language plpgsql;

-- select * from export_tools.varying_arr_to_int_arr(array[b'00000000', null, b'11111111'], 0);

drop table if exists export_tools.qr_mask_data;
create table export_tools.qr_mask_data (
    qr_correction_level export_tools.qr_correction_level_type,
    correction_level_bits bit varying(2),
    mask bit varying(3),
    xored_data bit varying(15)
);

insert into export_tools.qr_mask_data
values('M', B'00', B'000', B'101010000010010'),
       ('M', B'00', B'001', B'101000100100101'),
       ('M', B'00', B'010', B'101111001111100'),
       ('M', B'00', B'011', B'101101101001011'),
       ('M', B'00', B'100', B'100010111111001'),
       ('M', B'00', B'101', B'100000011001110'),
       ('M', B'00', B'110', B'100111110010111'),
       ('M', B'00', B'111', B'100101010100000'),
       --
       ('L', B'01', B'000', B'111011111000100'),
       ('L', B'01', B'001', B'111001011110011'),
       ('L', B'01', B'010', B'111110110101010'),
       ('L', B'01', B'011', B'111100010011101'),
       ('L', B'01', B'100', B'110011000101111'),
       ('L', B'01', B'101', B'110001100011000'),
       ('L', B'01', B'110', B'110110001000001'),
       ('L', B'01', B'111', B'110100101110110'),
       --
       ('H', B'10', B'000', B'001011010001001'),
       ('H', B'10', B'001', B'001001110111110'),
       ('H', B'10', B'010', B'001110011100111'),
       ('H', B'10', B'011', B'001100111010000'),
       ('H', B'10', B'100', B'000011101100010'),
       ('H', B'10', B'101', B'000001001010101'),
       ('H', B'10', B'110', B'000110100001100'),
       ('H', B'10', B'111', B'000100000111011'),
       --
       ('Q', B'11', B'000', B'011010101011111'),
       ('Q', B'11', B'001', B'011000001101000'),
       ('Q', B'11', B'010', B'011111100110001'),
       ('Q', B'11', B'011', B'011101000000110'),
       ('Q', B'11', B'100', B'010010010110100'),
       ('Q', B'11', B'101', B'010000110000011'),
       ('Q', B'11', B'110', B'010111011011010'),
       ('Q', B'11', B'111', B'010101111101101');

drop table if exists export_tools.qr_galua_coeffs;
create table export_tools.qr_galua_coeffs (
    value bit varying(8),
    galua bit varying(8)
);
insert into export_tools.qr_galua_coeffs
select v.v1::bit(8)::bit varying(8),
       v.v2::bit(8)::bit varying(8)
from (values (0, 1),
(1, 2),
(2, 4),
(3, 8),
(4, 16),
(5, 32),
(6, 64),
(7, 128),
(8, 29),
(9, 58),
(10, 116),
(11, 232),
(12, 205),
(13, 135),
(14, 19),
(15, 38),
(16, 76),
(17, 152),
(18, 45),
(19, 90),
(20, 180),
(21, 117),
(22, 234),
(23, 201),
(24, 143),
(25, 3),
(26, 6),
(27, 12),
(28, 24),
(29, 48),
(30, 96),
(31, 192),
(32, 157),
(33, 39),
(34, 78),
(35, 156),
(36, 37),
(37, 74),
(38, 148),
(39, 53),
(40, 106),
(41, 212),
(42, 181),
(43, 119),
(44, 238),
(45, 193),
(46, 159),
(47, 35),
(48, 70),
(49, 140),
(50, 5),
(51, 10),
(52, 20),
(53, 40),
(54, 80),
(55, 160),
(56, 93),
(57, 186),
(58, 105),
(59, 210),
(60, 185),
(61, 111),
(62, 222),
(63, 161),
(64, 95),
(65, 190),
(66, 97),
(67, 194),
(68, 153),
(69, 47),
(70, 94),
(71, 188),
(72, 101),
(73, 202),
(74, 137),
(75, 15),
(76, 30),
(77, 60),
(78, 120),
(79, 240),
(80, 253),
(81, 231),
(82, 211),
(83, 187),
(84, 107),
(85, 214),
(86, 177),
(87, 127),
(88, 254),
(89, 225),
(90, 223),
(91, 163),
(92, 91),
(93, 182),
(94, 113),
(95, 226),
(96, 217),
(97, 175),
(98, 67),
(99, 134),
(100, 17),
(101, 34),
(102, 68),
(103, 136),
(104, 13),
(105, 26),
(106, 52),
(107, 104),
(108, 208),
(109, 189),
(110, 103),
(111, 206),
(112, 129),
(113, 31),
(114, 62),
(115, 124),
(116, 248),
(117, 237),
(118, 199),
(119, 147),
(120, 59),
(121, 118),
(122, 236),
(123, 197),
(124, 151),
(125, 51),
(126, 102),
(127, 204),
(128, 133),
(129, 23),
(130, 46),
(131, 92),
(132, 184),
(133, 109),
(134, 218),
(135, 169),
(136, 79),
(137, 158),
(138, 33),
(139, 66),
(140, 132),
(141, 21),
(142, 42),
(143, 84),
(144, 168),
(145, 77),
(146, 154),
(147, 41),
(148, 82),
(149, 164),
(150, 85),
(151, 170),
(152, 73),
(153, 146),
(154, 57),
(155, 114),
(156, 228),
(157, 213),
(158, 183),
(159, 115),
(160, 230),
(161, 209),
(162, 191),
(163, 99),
(164, 198),
(165, 145),
(166, 63),
(167, 126),
(168, 252),
(169, 229),
(170, 215),
(171, 179),
(172, 123),
(173, 246),
(174, 241),
(175, 255),
(176, 227),
(177, 219),
(178, 171),
(179, 75),
(180, 150),
(181, 49),
(182, 98),
(183, 196),
(184, 149),
(185, 55),
(186, 110),
(187, 220),
(188, 165),
(189, 87),
(190, 174),
(191, 65),
(192, 130),
(193, 25),
(194, 50),
(195, 100),
(196, 200),
(197, 141),
(198, 7),
(199, 14),
(200, 28),
(201, 56),
(202, 112),
(203, 224),
(204, 221),
(205, 167),
(206, 83),
(207, 166),
(208, 81),
(209, 162),
(210, 89),
(211, 178),
(212, 121),
(213, 242),
(214, 249),
(215, 239),
(216, 195),
(217, 155),
(218, 43),
(219, 86),
(220, 172),
(221, 69),
(222, 138),
(223, 9),
(224, 18),
(225, 36),
(226, 72),
(227, 144),
(228, 61),
(229, 122),
(230, 244),
(231, 245),
(232, 247),
(233, 243),
(234, 251),
(235, 235),
(236, 203),
(237, 139),
(238, 11),
(239, 22),
(240, 44),
(241, 88),
(242, 176),
(243, 125),
(244, 250),
(245, 233),
(246, 207),
(247, 131),
(248, 27),
(249, 54),
(250, 108),
(251, 216),
(252, 173),
(253, 71),
(254, 142)) v(v1, v2);

drop function if exists export_tools.show_bit_arr_as_int_arr(_values bit varying(8)[]);
create or replace function export_tools.show_bit_arr_as_int_arr(_values bit varying(8)[]) returns int[] as $$
    begin
        return (select array_agg(i.el::bit(8)::int order by i.index) from unnest(_values) with ordinality i(el, index));
    end;
    $$ language plpgsql;

drop function if exists export_tools.qr_add_to_gen_array(gen_array bit varying(8)[], b bit varying(8));
create or replace function export_tools.qr_add_to_gen_array(gen_array bit varying(8)[], b bit varying(8)) returns bit varying(8)[] as $$
    declare coeff int;
    begin
        coeff = b::bit(8)::int;
        return (select array_agg(g.galua order by i.index)
                from unnest(gen_array) with ordinality i(el, index)
                         cross join lateral (select ((i.el::bit(8)::int + coeff) % 255)::bit(8)::bit varying(8)) t(v)
                         join export_tools.qr_galua_coeffs g on g.value = t.v
        );
    end;
    $$ language plpgsql;

drop function if exists export_tools.qr_add_gen_array_and_corr_bytes(gen_array bit varying(8)[], corr_bytes bit varying(8)[]);
create or replace function export_tools.qr_add_gen_array_and_corr_bytes(gen_array bit varying(8)[], corr_bytes bit varying(8)[]) returns bit varying(8)[] as $$
    declare current_index int4;
    begin
        for current_index in (select generate_subscripts(gen_array, 1)) loop
            corr_bytes[current_index] = corr_bytes[current_index]::bit(8) # gen_array[current_index]::bit(8);
        end loop;
        return corr_bytes;
    end;
    $$ language plpgsql;

select * from export_tools.qr_settings;

drop function if exists export_tools.qr_calc_corrs_bytes(data_block bit varying(8)[], corrs_block_count int4);
create or replace function export_tools.qr_calc_corrs_bytes(data_block  bit varying(8)[], corrs_block_count int4) returns bit varying(8)[] as $$
    declare corrs_block  bit varying(8)[];
    declare corrs_qr_gen_array bit varying(8)[];
    declare data_length int4;
    declare current_index int4;
    declare a bit varying(8);
    declare b bit varying(8);
    begin
        data_length = array_length(data_block, 1);
        corrs_block = data_block || (select array_agg(0::bit(8)::bit varying(8)) from generate_series(1, greatest(corrs_block_count - data_length, 0)));
        select a.gen_array into corrs_qr_gen_array from export_tools.qr_gen_array a where a.correction_byte_count = corrs_block_count;

        for current_index in (select generate_series(1, data_length)) loop
            a = corrs_block[1];
            if a is not null and a <> b'0'::bit varying(8) then
                b = (select c.value from export_tools.qr_galua_coeffs c where c.galua = a limit 1);
                corrs_block =  export_tools.qr_add_gen_array_and_corr_bytes(export_tools.qr_add_to_gen_array(corrs_qr_gen_array, b), corrs_block[2:] || array[b'0'::bit varying(8)]);
            else
                corrs_block = corrs_block[2:] || array[b'0'::bit varying(8)];
            end if;
        end loop;

        return corrs_block[:corrs_block_count];
    end;
$$ language plpgsql;

select export_tools.qr_calc_corrs_bytes(array[ 64, 196, 132,  84, 196, 196, 242, 194,   4, 132,  20,  37,  34,  16, 236,  17]::bit(8)[]::bit varying(8)[], 28);

drop function if exists export_tools.fill_qr_version(_qr_array int[][], _mask_data int[15]);
create or replace function export_tools.fill_qr_version(_qr_array int[][], _mask_data int[15]) returns int[][] as $$
    declare frame_width int4 = 4;
    declare qr_size int4;
    declare curr_coords record;
    begin

        qr_size = array_length(_qr_array, 1);
        _qr_array = export_tools.qr_insert_pic_to_array(_qr_array, frame_width + 9, qr_size - frame_width - 8 + 1, array[array[160 | 1]], true);

        for curr_coords in (select * from (values (15, frame_width + 9, frame_width + 1, qr_size - frame_width, frame_width + 9),
                                                  (14, frame_width + 9, frame_width + 2, qr_size - frame_width - 1, frame_width + 9),
                                                  (13, frame_width + 9, frame_width + 3, qr_size - frame_width - 2, frame_width + 9),
                                                  (12, frame_width + 9, frame_width + 4, qr_size - frame_width - 3, frame_width + 9),
                                                  (11, frame_width + 9, frame_width + 5, qr_size - frame_width - 4, frame_width + 9),
                                                  --
                                                  (10, frame_width + 9, frame_width + 6, qr_size - frame_width - 5, frame_width + 9),
                                                  (9, frame_width + 9, frame_width + 8, qr_size - frame_width - 6, frame_width + 9),
                                                  (8, frame_width + 9, frame_width + 9, qr_size - frame_width - 7, frame_width + 9),
                                                  (7, frame_width + 8, frame_width + 9, frame_width + 9, qr_size - frame_width - 8 + 2),
                                                  (6, frame_width + 8 - 2, frame_width + 9, frame_width + 9, qr_size - frame_width - 8 + 3),
                                                  --
                                                  (5, frame_width + 8 - 3, frame_width + 9, frame_width + 9, qr_size - frame_width - 8 + 4),
                                                  (4, frame_width + 8 - 4, frame_width + 9, frame_width + 9, qr_size - frame_width - 8 + 5),
                                                  (3, frame_width + 8 - 5, frame_width + 9, frame_width + 9, qr_size - frame_width - 8 + 6),
                                                  (2, frame_width + 8 - 6, frame_width + 9, frame_width + 9, qr_size - frame_width - 8 + 7),
                                                  (1, frame_width + 8 - 7, frame_width + 9, frame_width + 9, qr_size - frame_width - 8 + 8)

            ) v(index, x1, y1, x2, y2)) loop
            _mask_data[curr_coords.index] = _mask_data[curr_coords.index] | 160;
            _qr_array = export_tools.qr_insert_pic_to_array(_qr_array, curr_coords.x1, curr_coords.y1, array[array[_mask_data[curr_coords.index]]], true);
            _qr_array = export_tools.qr_insert_pic_to_array(_qr_array, curr_coords.x2, curr_coords.y2, array[array[_mask_data[curr_coords.index]]], true);
        end loop;
        return _qr_array;
    end;
    $$ language plpgsql;

drop function if exists export_tools.fill_qr_version_data(_qr_array int[][], _qr_correction_level export_tools.qr_correction_level_type, _mask int);
create or replace function export_tools.fill_qr_version_data(_qr_array int[][],  _qr_correction_level export_tools.qr_correction_level_type default 'M'::export_tools.qr_correction_level_type, _mask int default null::int) returns int[][] as $$
    begin
        return export_tools.fill_qr_version(_qr_array, export_tools.varying_arr_to_int_arr((
            select array[m.xored_data] from export_tools.qr_mask_data m where m.qr_correction_level = _qr_correction_level and m.mask = _mask::bit(3) limit 1
        ), 160));
    end;
    $$ language plpgsql;

drop function if exists export_tools.qr_fill_tech_info(_qr_version int4);
create or replace function export_tools.qr_fill_tech_info(_qr_version int4) returns int[][] as $$
    declare frame_width int4 = 4;
    declare qr_array int[][];
    declare qr_size int4;
    declare temp_array int[];
    declare form_coords record;
    declare qr_version_setting export_tools.qr_settings;
    begin
        if _qr_version < 1 or _qr_version > 40 then
            raise notice 'Невірна версія QR (%). Версія має бути в діапазоні 1..40', _qr_version;
            return qr_array;
        end if;
        qr_size = export_tools.qr_size(_qr_version) + frame_width * 2;
        raise notice 'qr_version %, qr_size %', _qr_version, qr_size;

        select * into qr_version_setting from export_tools.qr_settings s where s.qr_version = _qr_version limit 1;

        -- заливка отступа
        qr_array = export_tools.qr_insert_pic_to_array(
                export_tools.qr_form_array(qr_size, qr_size, 192),
                frame_width + 1, frame_width + 1,
               export_tools.qr_form_array(qr_size - frame_width * 2, qr_size - frame_width * 2),
            true
            );

         raise notice 'qr_array %', qr_array;
        -- отрисовка поисковых узоров
        for form_coords in (select * from (values(frame_width + 1, frame_width + 1, 0, 0), (qr_size - frame_width - 8 + 1, frame_width + 1, 1, 0), (frame_width + 1, qr_size - frame_width - 8 + 1, 0, 1)) v(x, y, x_i, y_i)) loop
            qr_array = export_tools.qr_insert_pic_to_array(qr_array, form_coords.x, form_coords.y, export_tools.qr_form_array(8, 8, 128));
            qr_array = export_tools.qr_insert_pic_to_array(qr_array, form_coords.x + form_coords.x_i, form_coords.y + form_coords.y_i, array[
                array[129, 129, 129, 129, 129, 129, 129],
                array[129, 128, 128, 128, 128, 128, 129],
                array[129, 128, 129, 129, 129, 128, 129],
                array[129, 128, 129, 129, 129, 128, 129],
                array[129, 128, 129, 129, 129, 128, 129],
                array[129, 128, 128, 128, 128, 128, 129],
                array[129, 129, 129, 129, 129, 129, 129]
            ], true);
        end loop;

        -- отрисовка выравнивающих узоров
        for form_coords in (
            select
                x.index as x, y.index as y
            from (select array_agg(p.pos + frame_width - 2 + 1) as coords from unnest(qr_version_setting.alignment_positions) p(pos)) ca
            cross join unnest(ca.coords) x(index)
            cross join unnest(ca.coords) y(index)
        ) loop
            qr_array = export_tools.qr_insert_pic_to_array(qr_array, form_coords.x, form_coords.y, array[
                array[129, 129, 129, 129, 129],
                array[129, 128, 128, 128, 129],
                array[129, 128, 129, 128, 129],
                array[129, 128, 128, 128, 129],
                array[129, 129, 129, 129, 129]
                ]);
        end loop;

        -- отрисовка полос синхронизации
        temp_array = (select array_agg(i.ind % 2 | 128) from generate_series(1, qr_size - frame_width * 2 - 16) i(ind));
--         raise notice 'temp_array %', temp_array;
        qr_array = export_tools.qr_insert_pic_to_array(qr_array, frame_width + 8 + 1, frame_width + 7, array[temp_array], true);
        qr_array = export_tools.qr_insert_pic_to_array(qr_array, frame_width + 7, frame_width + 8 + 1, export_tools.qr_transpose_array(array[temp_array]), true);

        -- информация про версию кода
        if array_length(qr_version_setting.code_version, 1) is not null then
            temp_array = (select array_agg(export_tools.varying_arr_to_int_arr(array[e.el], 64)) from unnest(qr_version_setting.code_version) e(el));
            qr_array = export_tools.qr_insert_pic_to_array(qr_array, frame_width + 1, qr_size - frame_width - 8 - 2, temp_array);
            qr_array = export_tools.qr_insert_pic_to_array(qr_array, qr_size - frame_width - 8 - 2, frame_width + 1, export_tools.qr_transpose_array(temp_array));
        end if;

        return qr_array;
    end;
    $$ language plpgsql;

 select
    string_agg((
        select string_agg(lpad(coalesce(t.arr[y.index][x.index]::text, ''), 3, ' '), ' ')
        from generate_subscripts(t.arr, 2) x(index)
                   ), E'\n')
from export_tools.qr_fill_tech_info(7) t(arr)
cross join generate_subscripts(t.arr, 1) y(index);

drop function if exists export_tools.show_qr_table_in_unicode(qr_table int[][]);
create or replace function export_tools.show_qr_table_in_unicode(qr_table int[][]) returns text as $$
    declare str text;
    begin
         select
            string_agg((
                select string_agg(case when coalesce(t.arr[y.index][x.index], 0) % 2 = 0 then E'\u2B1C' when export_tools.qr_is_editable_cell(t.arr[y.index][x.index]) then E'\u26AB' else E'\u2B1B' end, '')
                from generate_subscripts(t.arr, 2) x(index)
            ), E'\n')
            into str
        from (select qr_table) t(arr)
        cross join generate_subscripts(t.arr, 1) y(index);
        return str;
    end;
    $$ language plpgsql;

select export_tools.show_qr_table_in_unicode(export_tools.qr_fill_tech_info(1));

drop function if exists export_tools.qr_fill_empty_data_byte(bstr bytea, need_length int4);
create or replace function export_tools.qr_fill_empty_data_byte(bstr bytea, need_length int4) returns bytea as $$
    declare temp_str bytea;
    declare filled_length int4;
    declare ind int4 = 1;
    begin
        filled_length = greatest(need_length - length(bstr), 0);
        temp_str = coalesce(export_tools.form_empty_bytea(filled_length), ''::bytea);
        for ind in (select generate_series(1, filled_length)) loop
            temp_str = set_byte(temp_str, ind - 1, case when ind % 2 = 1 then b'11101100' else b'00010001' end ::int);
        end loop;
        return substring(bstr || temp_str for need_length);
    end;
    $$ language plpgsql;

select export_tools.qr_fill_empty_data_byte('http://jb.poe.pl.ua/1234'::bytea, 24);

drop function if exists export_tools.calc_block_length(data_length int4, block_count int4);
create or replace function export_tools.calc_block_length(data_length int4, block_count int4) returns int4[] as $$
    begin
        return (select array_agg(data_length / block_count + case when  g.index > (block_count - (data_length % block_count)) then 1 else 0 end) from generate_series(1, block_count) g(index));
    end;
    $$ language plpgsql;
select export_tools.calc_block_length(439, 34);

drop function if exists export_tools.prepare_data_and_corrs_blocks_stream(qr_content_data bit varying(8)[], qr_version_setting export_tools.qr_settings);
create or replace function export_tools.prepare_data_and_corrs_blocks_stream(qr_content_data bit varying(8)[], qr_version_setting export_tools.qr_settings) returns bit varying(8)[] as $$
    declare qr_stream bit varying(8)[];
    declare current_block_index int4;
    declare current_byte_index int4;
    declare block_lengths int4[];
    declare max_block_length int4;
    declare current_data_block bit varying(8)[];
    declare data_blocks bit varying(8)[][] = array[]::bit varying(8)[][];
    declare corrs_blocks bit varying(8)[][] = array[]::bit varying(8)[][];
    begin
        block_lengths = export_tools.calc_block_length(qr_version_setting.byte_length, qr_version_setting.block_count);
        max_block_length = (select max(l.len) from unnest(block_lengths) l(len));
        for current_block_index in (select generate_subscripts(block_lengths, 1)) loop
            current_data_block = qr_content_data[:block_lengths[current_block_index]]; -- || case when max_block_length = block_lengths[current_block_index] then array[]::bit varying(8)[] else array[null]::bit varying(8)[] end;
            corrs_blocks = corrs_blocks || array[export_tools.qr_calc_corrs_bytes(current_data_block, qr_version_setting.correction_byte_count)];
            raise notice 'calc_block_length % % % % % %', current_block_index, block_lengths, block_lengths[current_block_index], export_tools.show_bit_arr_as_int_arr(current_data_block),  export_tools.show_bit_arr_as_int_arr(export_tools.qr_calc_corrs_bytes(current_data_block, qr_version_setting.correction_byte_count)), export_tools.show_bit_arr_as_int_arr(qr_content_data);
            current_data_block = current_data_block || case when max_block_length = block_lengths[current_block_index] then array[]::bit varying(8)[] else array[null]::bit varying(8)[] end;
            data_blocks = data_blocks || array[current_data_block];
            qr_content_data = qr_content_data[block_lengths[current_block_index]+1:];
        end loop;

        qr_stream = array[]::bit varying(8)[];
        for current_byte_index in (select generate_series(1, max_block_length)) loop
            for current_block_index in (select generate_subscripts(data_blocks, 1)) loop
                if data_blocks[current_block_index][current_byte_index] is not null then
                    qr_stream = array_append(qr_stream, data_blocks[current_block_index][current_byte_index]);
                    raise notice 'data_blocks % % % % %', max_block_length, array_length(data_blocks, 1), current_block_index, current_byte_index, data_blocks[current_block_index][current_byte_index]::bit(8)::int;
                end if;
            end loop;
        end loop;
        for current_byte_index in (select generate_subscripts(corrs_blocks, 2)) loop
            for current_block_index in (select generate_subscripts(corrs_blocks, 1)) loop
                if corrs_blocks[current_block_index][current_byte_index] is not null then
                    qr_stream = array_append(qr_stream, corrs_blocks[current_block_index][current_byte_index]);
                    raise notice 'corrs_blocks % % % % %', array_length(corrs_blocks, 2), array_length(corrs_blocks, 1), current_block_index, current_byte_index, corrs_blocks[current_block_index][current_byte_index]::bit(8)::int;
                end if;
            end loop;
        end loop;

        return qr_stream;
    end;
    $$ language plpgsql;

select *, export_tools.show_qr_table_in_unicode(qr) from export_tools.qr_form_data_array('poltavaenergozbut'::bytea, 'M') t(qr);

select * from export_tools.qr_settings;

drop function if exists export_tools.qr_is_editable_cell(value int);
create or replace function export_tools.qr_is_editable_cell(value int) returns boolean as $$
    begin
        return value is null or (value >> 5) = 0;
    end;
    $$ language plpgsql;

drop function if exists export_tools.qr_is_masked_cell(value int);
create or replace function export_tools.qr_is_masked_cell(value int) returns boolean as $$
    begin
        return export_tools.qr_is_editable_cell(value);
--         return value is null or (value >> 7) = 0;
    end;
    $$ language plpgsql;


select export_tools.qr_is_editable_cell(64);
select export_tools.qr_is_masked_cell(64);

drop function if exists export_tools.qr_fill_data_stream(_qr_array int[][],  _qr_content_array int[]);
create or replace function export_tools.qr_fill_data_stream(_qr_array int[][],  _qr_content_array int[]) returns int[][] as $$
    declare _qr_size int4;
    declare frame_width int4 = 4;
    declare x int4;
    declare y int4;
    declare byte_index int4;
    declare data_length int4;
    declare base_qr_size int4;
    declare base_index_x int4;
    declare base_index_y int4;
    declare current_column int4;
    declare route_up boolean;
    declare need_change_route boolean = false;
    begin
        _qr_size = array_length(_qr_array, 1);
        base_qr_size = _qr_size - frame_width * 2;
        base_index_x = _qr_size - frame_width;
        base_index_y = _qr_size - frame_width;
        x = 0;
        y = 0;
        current_column = 0;
        route_up = true;

        byte_index = 1;
        data_length = array_length(_qr_content_array, 1);
        while (x < base_qr_size and y < base_qr_size) and byte_index <= data_length loop
--             raise notice 'qr_fill_data_stream % % % % % % % %', byte_index, x, y, need_change_route, route_up, base_index_x - x, base_index_y - y, _qr_content_array;
            if need_change_route then
                route_up = not route_up;
                current_column = current_column + 2;
                if (base_index_x - current_column) = 11 then
                    base_index_x = base_index_x - 1;
                end if;
                x = current_column + ((x) % 2);
                if export_tools.qr_is_editable_cell(_qr_array[base_index_y - y][base_index_x - x]) then
                    _qr_array[base_index_y - y][base_index_x - x] = _qr_content_array[byte_index];
--                     _qr_array[base_index_y - y][base_index_x - x] =  byte_index;
                    byte_index = byte_index + 1;
--                     raise notice 'qr_is_editable_cell % % % % % %', byte_index, x, y, base_index_x - x, base_index_y - y, _qr_content_array[byte_index];
                end if;
--                 raise notice 'need_change_route % % % % % % % % %', byte_index, x, y, current_column, need_change_route, route_up, base_index_x - x, base_index_y - y, _qr_content_array;
            else
                if export_tools.qr_is_editable_cell(_qr_array[base_index_y - y][base_index_x - x]) then
                    _qr_array[base_index_y - y][base_index_x - x] =  _qr_content_array[byte_index];
--                     _qr_array[base_index_y - y][base_index_x - x] =  byte_index;
                    byte_index = byte_index + 1;
--                     raise notice 'qr_is_editable_cell % % % % % %', byte_index, x, y, base_index_x - x, base_index_y - y, _qr_content_array[byte_index];
                end if;
                y = y + case when route_up then 1 else -1 end * (x % 2);
            end if;
            x = current_column + ((x + 1) % 2);
            need_change_route = (y < 0 or y >= base_qr_size) and not need_change_route;
            y = greatest(least(y, base_qr_size - 1), 0);
        end loop;

        return _qr_array;
    end;
    $$ language plpgsql;

 select
    string_agg((
        select string_agg(lpad(coalesce(case when t.arr[y.index][x.index] not in (192, 128, 129, 160, 161) then t.arr[y.index][x.index]::text end, ''), 3, ' '), ' ')
        from generate_subscripts(t.arr, 2) x(index)
                   ), E'\n')
from export_tools.qr_form_data_array('poltavaenergozbut poltavaoblenergo'::bytea, 'M') t(arr)
cross join generate_subscripts(t.arr, 1) y(index);

 select
    string_agg((
        select string_agg(lpad(coalesce(t.arr[y.index][x.index]::text, ''), 3, ' '), ' ')
        from generate_subscripts(t.arr, 2) x(index)
                   ), E'\n')
from export_tools.qr_form_data_array('poltavaenergozbut'::bytea, 'M') t(arr)
cross join generate_subscripts(t.arr, 1) y(index);

select export_tools.show_qr_table_in_unicode(qr) from export_tools.qr_form_data_array('poltavaenergozbut'::bytea, 'M') t(qr);

drop function if exists export_tools.qr_mask_data(_qr_array int[][], _qr_correction_level export_tools.qr_correction_level_type, _mask int4);
create or replace function export_tools.qr_mask_data(_qr_array int[][], _qr_correction_level export_tools.qr_correction_level_type, _mask int4) returns int[][] as $$
    declare frame_width int4 = 4;
    declare qr_size int4;
    declare x int4;
    declare y int4;

    begin
        _qr_array = export_tools.fill_qr_version_data( _qr_array, _qr_correction_level, _mask);
        qr_size = array_length(_qr_array, 1) - frame_width * 2;
        frame_width = frame_width + 1;
        for y in (select generate_series(0, qr_size - 1)) loop
             for x in(select generate_series(0, qr_size - 1)) loop
                if export_tools.qr_is_masked_cell(_qr_array[frame_width + y][frame_width + x] ) then
                    _qr_array[frame_width + y][frame_width + x] = case when (case _mask
                        when 0 then (x + y) % 2
                        when 1 then y % 2
                        when 2 then x % 3
                        when 3 then (x + y) % 3
                        when 4 then (x/3 + y/2) % 2
                        when 5 then (x * y) % 2 + (x * y) % 3
                        when 6 then ((x * y) % 3 + (x * y)) % 2
                        when 7 then ((x * y) % 3 + x + y) % 2
                    end = 0) then ((coalesce(_qr_array[frame_width + y][frame_width + x], 0) >> 1) << 1) + ((coalesce(_qr_array[frame_width + y][frame_width + x], 0) + 1) % 2) else coalesce(_qr_array[frame_width + y][frame_width + x], 0) end;
                end if;
            end loop;
        end loop;

        return _qr_array;
    end;
    $$ language plpgsql;


drop function if exists export_tools.qr_form_data_array(_qr_content bytea, _qr_correction_level export_tools.qr_correction_level_type);
create or replace function export_tools.qr_form_data_array(_qr_content bytea, _qr_correction_level export_tools.qr_correction_level_type default 'M'::export_tools.qr_correction_level_type) returns int[][] as $$
    declare _qr_array int[][];
    declare _qr_version int4;
    declare _qr_content_length int4;
    declare _qr_content_data bit varying(8)[];
    declare _qr_content_array int[];
    declare qr_version_setting export_tools.qr_settings;

    declare is_valid_version boolean;
    begin
        _qr_content_length = length(_qr_content);
        _qr_version = export_tools.get_qr_version(_qr_content_length,_qr_correction_level) - 1;

        is_valid_version = false;
        while not is_valid_version loop
            _qr_version = _qr_version + 1;
            select * into qr_version_setting from export_tools.qr_settings s where s.qr_version = _qr_version and s.qr_correction_level = _qr_correction_level limit 1;

            _qr_content_data = export_tools.bytea_to_bit_varying_arr(
                export_tools.qr_fill_empty_data_byte(
                    export_tools.json_bit_varying_arr_to_bytea(array[
                            b'0100'::bit varying(4),
                            case when qr_version_setting.bit_data_length = 8 then _qr_content_length::bit(8)::bit varying(8) else  _qr_content_length::bit(16)::bit varying(16) end
                        ] || export_tools.bytea_to_bit_varying_arr(_qr_content) || array[b'0000'::bit varying(4)]), qr_version_setting.byte_length
                    )
                );
            is_valid_version = array_length(_qr_content_data, 1) <= qr_version_setting.byte_length;
        end loop;
        _qr_content_array = export_tools.varying_arr_to_int_arr(export_tools.prepare_data_and_corrs_blocks_stream(_qr_content_data, qr_version_setting));

        raise notice 'qr_form_data_array % % % % % %', _qr_content, _qr_content_length, _qr_correction_level, _qr_version, _qr_content_data, export_tools.json_bit_varying_arr_to_bytea(array[
                        b'0100'::bit varying(4),
                        case when qr_version_setting.bit_data_length = 8 then _qr_content_length::bit(8)::bit varying(8) else  _qr_content_length::bit(16)::bit varying(16) end
                    ] || export_tools.bytea_to_bit_varying_arr(_qr_content) || array[b'0000'::bit varying(4)]);
        _qr_array = export_tools.qr_fill_tech_info(_qr_version);
        _qr_array = export_tools.fill_qr_version(_qr_array, (select array_agg(0) from generate_series(1, 18) g));
        _qr_array = export_tools.qr_fill_data_stream(_qr_array, _qr_content_array);
        _qr_array = export_tools.qr_mask_data(_qr_array, _qr_correction_level, 1);

        return _qr_array;
    end;
    $$ language plpgsql;


select * from export_tools.qr_settings where qr_version = 7;
select * from export_tools.qr_gen_array where correction_byte_count = 26;

-- select *, export_tools.calc_block_length(byte_length, block_count) from export_tools.qr_settings where qr_version = 2;

select *, export_tools.show_qr_table_in_unicode(qr) from export_tools.qr_form_data_array('poltavaenergozbut'::bytea, 'M') t(qr);

select
    export_tools.show_qr_table_in_unicode(qr),
--     export_tools.show_qr_table_in_unicode(m0.m),
--     export_tools.show_qr_table_in_unicode(m1.m),
--     export_tools.show_qr_table_in_unicode(m2.m),
--     export_tools.show_qr_table_in_unicode(m3.m),
--     export_tools.show_qr_table_in_unicode(m4.m),
--     export_tools.show_qr_table_in_unicode(m5.m),
--     export_tools.show_qr_table_in_unicode(m6.m),
--     export_tools.show_qr_table_in_unicode(m7.m),
    *
from export_tools.qr_form_data_array('Боровик Лілія Миколаївна https://www.olx.ua/d/uk/obyavlenie/2-kmnatna-kvartira-na-podol-vroremont-IDLSdxt.html'::bytea, 'M') t(qr)
-- from export_tools.qr_fill_tech_info(1) t(qr)
-- cross join export_tools.qr_mask_data(t.qr, 'M', 0) m0(m)
-- cross join export_tools.qr_mask_data(t.qr, 'M', 1) m1(m)
-- cross join export_tools.qr_mask_data(t.qr, 'M', 2) m2(m)
-- cross join export_tools.qr_mask_data(t.qr, 'M', 3) m3(m)
-- cross join export_tools.qr_mask_data(t.qr, 'M', 4) m4(m)
-- cross join export_tools.qr_mask_data(t.qr, 'M', 5) m5(m)
-- cross join export_tools.qr_mask_data(t.qr, 'M', 6) m6(m)
-- cross join export_tools.qr_mask_data(t.qr, 'M', 7) m7(m)
;

select * from export_tools.qr_mask_data;

select * from export_tools.bytea_to_bit_varying_arr('poltavaenergozbut'::bytea);
