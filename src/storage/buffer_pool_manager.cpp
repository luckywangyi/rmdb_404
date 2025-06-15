/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "buffer_pool_manager.h"

/**
 * @description: 从free_list或replacer中得到可淘汰帧页的 *frame_id
 * @return {bool} true: 可替换帧查找成功 , false: 可替换帧查找失败
 * @param {frame_id_t*} frame_id 帧页id指针,返回成功找到的可替换帧id
 */
bool BufferPoolManager::find_victim_page(frame_id_t *frame_id) {
    if (!free_list_.empty()) {
        *frame_id = free_list_.front();
        free_list_.pop_front();
        return true;
    }

    // 避免在持有缓冲池锁时调用replacer
    {
        std::scoped_lock replacer_lock{replacer_latch_};
        if (replacer_->Size() > 0 && replacer_->victim(frame_id)) {
            return true;
        }
    }

    return false;
}

/**
 * @description: 更新页面数据, 如果为脏页则需写入磁盘，再更新为新页面，更新page元数据(data, is_dirty, page_id)和page table
 * @param {Page*} page 写回页指针
 * @param {PageId} new_page_id 新的page_id
 * @param {frame_id_t} new_frame_id 新的帧frame_id
 */
void BufferPoolManager::update_page(Page *page, PageId new_page_id, frame_id_t new_frame_id) {
    // 1 如果是脏页，写回磁盘，并且把dirty置为false
    if (page->is_dirty_) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }

    // 2 更新page table：先移除旧映射再添加新映射
    page_table_.erase(page->id_);
    page_table_[new_page_id] = new_frame_id;

    // 3 重置page的data，更新page id
    page->reset_memory();
    page->id_ = new_page_id;
}

/**
 * @description: 从buffer pool获取需要的页。
 *              如果页表中存在page_id（说明该page在缓冲池中），并且pin_count++。
 *              如果页表不存在page_id（说明该page在磁盘中），则找缓冲池victim page，将其替换为磁盘中读取的page，pin_count置1。
 * @return {Page*} 若获得了需要的页则将其返回，否则返回nullptr
 * @param {PageId} page_id 需要获取的页的PageId
 */
/**
 * @description: 从buffer pool获取需要的页。
 */
Page *BufferPoolManager::fetch_page(PageId page_id) {
    std::scoped_lock lock{latch_};

    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
        frame_id_t frame_id = it->second;
        Page *page = &pages_[frame_id];
        page->pin_count_++;
        replacer_->pin(frame_id);
        return page;
    }

    frame_id_t frame_id;
    if (!find_victim_page(&frame_id)) {
        return nullptr;
    }
    Page *page = &pages_[frame_id];

    // 原子性处理脏页写回
    if (page->is_dirty_) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }

    // 清除旧页表项
    page_table_.erase(page->id_);

    // 读取新页面数据
    disk_manager_->read_page(page_id.fd, page_id.page_no, page->data_, PAGE_SIZE);

    // 更新页面元数据
    page->id_ = page_id;
    page->pin_count_ = 1;
    page_table_[page_id] = frame_id;
    replacer_->pin(frame_id);

    return page;
}

/**
 * @description: 取消固定pin_count>0的在缓冲池中的page
 */
bool BufferPoolManager::unpin_page(PageId page_id, bool is_dirty) {
    std::scoped_lock lock{latch_};

    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;
    }

    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];

    if (page->pin_count_ <= 0) {
        return false;
    }

    page->pin_count_--;

    // 原子性设置脏页标志
    if (is_dirty) {
        page->is_dirty_ = true;
    }

    if (page->pin_count_ == 0) {
        replacer_->unpin(frame_id);
    }

    return true;
}

/**
 * @description: 将目标页写回磁盘，不考虑当前页面是否正在被使用
 * @return {bool} 成功则返回true，否则返回false(只有page_table_中没有目标页时)
 * @param {PageId} page_id 目标页的page_id，不能为INVALID_PAGE_ID
 */
bool BufferPoolManager::flush_page(PageId page_id) {
    // Todo:
    // 0. lock latch
    // 1. 查找页表,尝试获取目标页P
    // 1.1 目标页P没有被page_table_记录 ，返回false
    // 2. 无论P是否为脏都将其写回磁盘。
    // 3. 更新P的is_dirty_
    // 1. 查找页表,尝试获取目标页P
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false; // 目标页P没有被page_table_记录
    }

    // 获取目标页的frame_id和对应的Page对象
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];

    // 2. 无论P是否为脏都将其写回磁盘
    disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);

    // 3. 更新P的is_dirty_
    page->is_dirty_ = false;
    return true;
}

/**
 * @description: 创建一个新的page，即从磁盘中移动一个新建的空page到缓冲池某个位置。
 * @return {Page*} 返回新创建的page，若创建失败则返回nullptr
 * @param {PageId*} page_id 当成功创建一个新的page时存储其page_id
 */
Page *BufferPoolManager::new_page(PageId *page_id) {
    std::scoped_lock lock{latch_};

    frame_id_t frame_id;
    if (!find_victim_page(&frame_id)) {
        return nullptr;
    }

    // 分配新页面ID（修复：直接使用传入的文件描述符）
    page_id_t new_page_no = disk_manager_->allocate_page(page_id->fd);
    *page_id = PageId{page_id->fd, new_page_no};

    Page* page = &pages_[frame_id];
    // 更新页面前处理旧数据（修复：调用标准更新流程）
    update_page(page, *page_id, frame_id);

    // 初始化新页面属性（修复：移除多余的磁盘写入）
    page->pin_count_ = 1;
    replacer_->pin(frame_id);

    return page;
}

/**
 * @description: 从buffer_pool删除目标页
 * @return {bool} 如果目标页不存在于buffer_pool或者成功被删除则返回true，若其存在于buffer_pool但无法删除则返回false
 * @param {PageId} page_id 目标页
 */
bool BufferPoolManager::delete_page(PageId page_id) {
    // 1.   在page_table_中查找目标页，若不存在返回true
    // 2.   若目标页的pin_count不为0，则返回false
    // 3.   将目标页数据写回磁盘，从页表中删除目标页，重置其元数据，将其加入free_list_，返回true
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return true; // 目标页不存在于缓冲池
    }

    // 获取目标页的frame_id和对应的Page对象
    frame_id_t frame_id = it->second;
    Page *page = &pages_[frame_id];

    // 2. 若目标页的pin_count不为0，则返回false
    if (page->pin_count_ > 0) {
        return false; // 目标页正在被使用，无法删除
    }

    // 3. 将目标页数据写回磁盘
    if (page->is_dirty_) {
        disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
        page->is_dirty_ = false;
    }

    // 从页表中删除目标页
    page_table_.erase(page_id);

    // 重置目标页的元数据
    page->reset_memory();
    page->id_ = PageId{-1, INVALID_PAGE_ID};
    page->pin_count_ = 0;

    // 将目标页加入free_list_
    free_list_.push_back(frame_id);

    return true; // 删除成功
}

/**
 * @description: 将buffer_pool中的所有页写回到磁盘
 * @param {int} fd 文件句柄
 */
void BufferPoolManager::flush_all_pages(int fd) {
    // 遍历页表中的所有页面
    for (const auto &entry : page_table_) {
        PageId page_id = entry.first;
        frame_id_t frame_id = entry.second;
        Page *page = &pages_[frame_id];

        // 检查页面是否属于指定文件句柄
        if (page_id.fd == fd) {
            // 将页面写回磁盘
            disk_manager_->write_page(page->id_.fd, page->id_.page_no, page->data_, PAGE_SIZE);
            // 更新页面的脏标记
            page->is_dirty_ = false;
        }
    }
}
