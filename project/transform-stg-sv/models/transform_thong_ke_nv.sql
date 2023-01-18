with tiep_nhan_tong as (
select dbo.new_uuid() as id, ma_dv_th, ten_dv_th, ky_du_lieu, 'tiep_nhan_tong' as param_key, sum(param_value) as param_value, 'P001' as group_code
from (
select ma_dv_th, ten_dv_th, ky_du_lieu, cast(so_luong as int) as param_value from {{ source('linh_vuc_13', 'thong_ke_nv') }}
where tinh_trang_xu_ly in (N'Đang xử lý', N'Đã xử lý')
) t0
group by ma_dv_th, ten_dv_th, ky_du_lieu
),
dang_xu_ly_tong as (
select dbo.new_uuid() as id, ma_dv_th, ten_dv_th, ky_du_lieu, 'dang_xu_ly_tong' as param_key, param_value, 'P001' as group_code
from (
select ma_dv_th, ten_dv_th, ky_du_lieu, cast(so_luong as int) as param_value from {{ source('linh_vuc_13', 'thong_ke_nv') }}
where tinh_trang_xu_ly = N'Đang xử lý'
) t0
),
dang_xu_ly_trong_han as (
select dbo.new_uuid() as id, ma_dv_th, ten_dv_th, ky_du_lieu, 'dang_xu_ly_trong_han' as param_key, param_value, 'P001' as group_code
from (
select ma_dv_th, ten_dv_th, ky_du_lieu, cast(so_luong as int) as param_value from {{ source('linh_vuc_13', 'thong_ke_nv') }}
where tinh_trang_xu_ly = N'Đang xử lý' and tinh_trang_han = N'Trong hạn'
) t0
),
dang_xu_ly_gan_han as (
select dbo.new_uuid() as id, ma_dv_th, ten_dv_th, ky_du_lieu, 'dang_xu_ly_gan_han' as param_key, param_value, 'P001' as group_code 
from (
select ma_dv_th, ten_dv_th, ky_du_lieu, cast(so_luong as int) as param_value from {{ source('linh_vuc_13', 'thong_ke_nv') }}
where tinh_trang_xu_ly = N'Đang xử lý' and tinh_trang_han = N'Gần hạn'
) t0
),
dang_xu_ly_qua_han as (
select dbo.new_uuid() as id, ma_dv_th, ten_dv_th, ky_du_lieu, 'dang_xu_ly_qua_han' as param_key, param_value, 'P001' as group_code 
from (
select ma_dv_th, ten_dv_th, ky_du_lieu, cast(so_luong as int) as param_value from {{ source('linh_vuc_13', 'thong_ke_nv') }}
where tinh_trang_xu_ly = N'Đang xử lý' and tinh_trang_han = N'Quá hạn'
) t0
),
da_xu_ly_tong as (
select dbo.new_uuid() as id, ma_dv_th, ten_dv_th, ky_du_lieu, 'da_xu_ly_tong' as param_key, param_value, 'P001' as group_code 
from (
select ma_dv_th, ten_dv_th, ky_du_lieu, cast(so_luong as int) as param_value from {{ source('linh_vuc_13', 'thong_ke_nv') }}
where tinh_trang_xu_ly = N'Đã xử lý'
) t0 
),
da_xu_ly_truoc_han as (
select dbo.new_uuid() as id, ma_dv_th, ten_dv_th, ky_du_lieu, 'da_xu_ly_truoc_han' as param_key, param_value, 'P001' as group_code 
from (
select ma_dv_th, ten_dv_th, ky_du_lieu, cast(so_luong as int) as param_value from {{ source('linh_vuc_13', 'thong_ke_nv') }}
where tinh_trang_xu_ly = N'Đã xử lý' and tinh_trang_han = N'Trước hạn'
) t0  
),
da_xu_ly_qua_han as (
select dbo.new_uuid() as id, ma_dv_th, ten_dv_th, ky_du_lieu, 'da_xu_ly_qua_han' as param_key, param_value, 'P001' as group_code 
from (
select ma_dv_th, ten_dv_th, ky_du_lieu, cast(so_luong as int) as param_value from {{ source('linh_vuc_13', 'thong_ke_nv') }}
where tinh_trang_xu_ly = N'Đã xử lý' and tinh_trang_han = N'Quá hạn'
) t0  
),
ty_le as (
select dbo.new_uuid() as id, ma_dv_th, ten_dv_th, ky_du_lieu, 'ty_le' as param_key, param_value, 'P001' as group_code 
from (
select t1.ma_dv_th, t1.ten_dv_th, t1.ky_du_lieu, t1.param_value/t2.param_value as param_value
from da_xu_ly_tong t1 left join tiep_nhan_tong t2 
on t1.ma_dv_th = t2.ma_dv_th and t1.ten_dv_th = t2.ten_dv_th and t1.ky_du_lieu = t2.ky_du_lieu
) t0
)

select id, ma_dv_th, ten_dv_th, ky_du_lieu, param_key, param_value, group_code from tiep_nhan_tong
union
select id, ma_dv_th, ten_dv_th, ky_du_lieu, param_key, param_value, group_code from dang_xu_ly_tong
union
select id, ma_dv_th, ten_dv_th, ky_du_lieu, param_key, param_value, group_code from dang_xu_ly_trong_han
union
select id, ma_dv_th, ten_dv_th, ky_du_lieu, param_key, param_value, group_code from dang_xu_ly_gan_han
union
select id, ma_dv_th, ten_dv_th, ky_du_lieu, param_key, param_value, group_code from dang_xu_ly_qua_han
union
select id, ma_dv_th, ten_dv_th, ky_du_lieu, param_key, param_value, group_code from da_xu_ly_tong
union
select id, ma_dv_th, ten_dv_th, ky_du_lieu, param_key, param_value, group_code from da_xu_ly_truoc_han
union
select id, ma_dv_th, ten_dv_th, ky_du_lieu, param_key, param_value, group_code from da_xu_ly_qua_han
union
select id, ma_dv_th, ten_dv_th, ky_du_lieu, param_key, param_value, group_code from ty_le
