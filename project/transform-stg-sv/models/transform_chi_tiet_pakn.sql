select * from (
select dbo.new_uuid() id, ma_pakn, ma_linh_vuc, ten_linh_vuc, loai_ho_so, ho_va_ten, dia_chi, email, dien_thoai, tieu_de, noi_dung,
        ten_tep, loai_pakn, ngay_gui_pakn, tieu_de_tra_loi, noi_dung_tra_loi, so_ngay_toi_han, ngay_nhan_xu_ly, ngay_hen_tra_loi,
        don_vi_tiep_nhan, don_vi_dieu_phoi, nguon_tiep_nhan, 'N005' as group_code
from {{ source('linh_vuc_12', 'chi_tiet_pakn') }}
) t0