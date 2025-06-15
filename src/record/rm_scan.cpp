#include "rm_scan.h"
#include "rm_file_handle.h"

RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle) {
    // 初始化 rid_，指向第一个存放记录的位置
    rid_.page_no = 0;
    rid_.slot_no = Bitmap::first_bit(true, file_handle_->fetch_page_handle(0).bitmap, file_handle_->file_hdr_.num_records_per_page);
}

void RmScan::next() {
    // 找到下一个存放记录的非空闲位置
    const int max_pages = file_handle_->file_hdr_.num_pages;
    while (rid_.page_no < max_pages) {
        RmPageHandle page_handle = file_handle_->fetch_page_handle(rid_.page_no);
        int next_slot = Bitmap::next_bit(true, page_handle.bitmap, file_handle_->file_hdr_.num_records_per_page, rid_.slot_no);
        if (next_slot != -1) {
            rid_.slot_no = next_slot;
            return;
        }
        rid_.page_no++;
        rid_.slot_no = -1; // 重置 slot_no，开始下一页的扫描
    }
}

bool RmScan::is_end() const {
    // 判断是否到达文件末尾
    return rid_.page_no >= file_handle_->file_hdr_.num_pages;
}

Rid RmScan::rid() const {
    // 返回当前 rid_
    return rid_;
}