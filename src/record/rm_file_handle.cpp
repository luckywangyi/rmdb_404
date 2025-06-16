/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_file_handle.h"

/**
 * @description: 获取当前表中记录号为rid的记录
 * @param {Rid&} rid 记录号，指定记录的位置
 * @param {Context*} context
 * @return {unique_ptr<RmRecord>} rid对应的记录对象指针
 */
std::unique_ptr<RmRecord> RmFileHandle::get_record(const Rid& rid, Context* context) const {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);  // fetch时会pin page
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);  // 保证记录有效
    }
    auto record = std::make_unique<RmRecord>(file_hdr_.record_size, page_handle.get_slot(rid.slot_no));
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), false);
    return record;
}

/**
 * @description: 在当前表中插入一条记录，不指定插入位置
 * @param {char*} buf 要插入的记录的数据
 * @param {Context*} context
 * @return {Rid} 插入的记录的记录号（位置）
 */
Rid RmFileHandle::insert_record(char* buf, Context* context) {
    RmPageHandle page_handle = create_page_handle();  // 获取当前未满的page handle
    int slot_no = Bitmap::first_bit(false, page_handle.bitmap, file_hdr_.num_records_per_page);  // 找到空闲slot位置
    memcpy(page_handle.get_slot(slot_no), buf, file_hdr_.record_size);  // 将buf复制到空闲slot位置
    Bitmap::set(page_handle.bitmap, slot_no);  // 更新bitmap
    page_handle.page_hdr->num_records++;
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
        file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;  // 页面已满
    }
    Rid rid{page_handle.page->get_page_id().page_no, slot_no};  // 记录号
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);  // 更新后unpin
    return rid;
}

/**
 * @description: 在当前表中的指定位置插入一条记录
 * @param {Rid&} rid 要插入记录的位置
 * @param {char*} buf 要插入记录的数据
 */
void RmFileHandle::insert_record(const Rid& rid, char* buf) {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        Bitmap::set(page_handle.bitmap, rid.slot_no);  // 更新bitmap
        page_handle.page_hdr->num_records++;
        if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page) {
            file_hdr_.first_free_page_no = page_handle.page_hdr->next_free_page_no;  // 页面已满
        }
    }
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);  // 将buf复制到空闲slot位置
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);  // 更新后unpin
}

/**
 * @description: 删除记录文件中记录号为rid的记录
 * @param {Rid&} rid 要删除的记录的记录号（位置）
 * @param {Context*} context
 */
void RmFileHandle::delete_record(const Rid& rid, Context* context) {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    Bitmap::reset(page_handle.bitmap, rid.slot_no);  // 更新bitmap
    page_handle.page_hdr->num_records--;
    if (page_handle.page_hdr->num_records == file_hdr_.num_records_per_page - 1) {
        release_page_handle(page_handle);  // 页面未满
    }
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);  // 更新后unpin
}

/**
 * @description: 更新记录文件中记录号为rid的记录
 * @param {Rid&} rid 要更新的记录的记录号（位置）
 * @param {char*} buf 新记录的数据
 * @param {Context*} context
 */
void RmFileHandle::update_record(const Rid& rid, char* buf, Context* context) {
    RmPageHandle page_handle = fetch_page_handle(rid.page_no);
    if (!Bitmap::is_set(page_handle.bitmap, rid.slot_no)) {
        throw RecordNotFoundError(rid.page_no, rid.slot_no);
    }
    memcpy(page_handle.get_slot(rid.slot_no), buf, file_hdr_.record_size);  // 更新记录
    buffer_pool_manager_->unpin_page(page_handle.page->get_page_id(), true);  // 更新后unpin
}

/**
 * @description: 获取指定页面的页面句柄
 * @param {int} page_no 页面号
 * @return {RmPageHandle} 指定页面的句柄
 */
RmPageHandle RmFileHandle::fetch_page_handle(int page_no) const {
    if (page_no < 0 || page_no >= file_hdr_.num_pages) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_), page_no);
    }
    PageId page_id{fd_, page_no};
    Page* page = buffer_pool_manager_->fetch_page(page_id);  // 缓冲池获取页面
    if (page == nullptr) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_), page_no);
    }
    return RmPageHandle(&file_hdr_, page);
}

/**
 * @description: 创建一个新的page handle
 * @return {RmPageHandle} 新的PageHandle
 */
RmPageHandle RmFileHandle::create_new_page_handle() {
    PageId page_id{fd_, INVALID_PAGE_ID};
    Page* page = buffer_pool_manager_->new_page(&page_id);
    if (page == nullptr) {
        throw PageNotExistError(disk_manager_->get_file_name(fd_), page_id.page_no);
    }
    RmPageHandle page_handle(&file_hdr_, page);
    page_handle.page_hdr->num_records = 0;
    page_handle.page_hdr->next_free_page_no = RM_NO_PAGE;
    Bitmap::init(page_handle.bitmap, file_hdr_.bitmap_size);  // 初始化bitmap
    file_hdr_.num_pages++;
    file_hdr_.first_free_page_no = page->get_page_id().page_no;  // 更新文件头
    return page_handle;
}

/**
 * @description: 创建或获取一个空闲的page handle
 * @return {RmPageHandle} 返回生成的空闲page handle
 */
RmPageHandle RmFileHandle::create_page_handle() {
    if (file_hdr_.first_free_page_no == RM_NO_PAGE) {
        return create_new_page_handle();  // 没有空闲页
    }
    return fetch_page_handle(file_hdr_.first_free_page_no);  // 有空闲页返回空闲页
}

/**
 * @description: 当一个页面从没有空闲空间的状态变为有空闲空间状态时，更新文件头和页头中空闲页面相关的元数据
 */
void RmFileHandle::release_page_handle(RmPageHandle& page_handle) {
    page_handle.page_hdr->next_free_page_no = file_hdr_.first_free_page_no;
    file_hdr_.first_free_page_no = page_handle.page->get_page_id().page_no;
}