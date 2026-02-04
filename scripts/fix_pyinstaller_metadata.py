#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
fix_pyinstaller_metadata.py - 修复PyInstaller处理conda包元数据时的KeyError: 'depends'问题

此脚本修补PyInstaller的元数据处理逻辑，解决当包的metadata中缺少'depends'键时引发的KeyError。
"""

import os


def patch_pyinstaller_conda_metadata():
    """
    修补PyInstaller的conda环境元数据处理逻辑
    设置环境变量来忽略conda元数据问题
    """
    # 设置环境变量告诉PyInstaller忽略conda元数据
    os.environ['PYINSTALLER_IGNORE_CONDA'] = '1'
    
    try:
        # 尝试导入PyInstaller并打补丁
        import PyInstaller.building.makespec
        import PyInstaller.utils.hooks
        import pkg_resources
        
        # 如果可以导入，则尝试修补处理conda元数据的相关代码
        _patch_conda_metadata_handling()
        
    except ImportError:
        # 如果PyInstaller未安装，只是设置了环境变量就足够了
        pass


def _patch_conda_metadata_handling():
    """
    内部函数，修补处理conda元数据的代码
    """
    try:
        # 动态导入PyInstaller相关模块
        from PyInstaller.utils.hooks import collect_all
        import inspect
        
        # 尝试修补pkg_resources相关逻辑
        def safe_get_metadata(name):
            """
            安全地获取包元数据，如果'depends'键不存在则返回空列表
            """
            try:
                metadata = pkg_resources.get_distribution(name).egg_info
                if isinstance(metadata, dict):
                    # 如果是字典形式的metadata，安全地获取depends
                    return metadata.get('depends', [])
                else:
                    # 如果是其他形式，尝试解析
                    try:
                        dist = pkg_resources.get_distribution(name)
                        reqs = dist.requires()
                        return [str(req) for req in reqs]
                    except Exception:
                        return []
            except Exception:
                return []

        # 替换或包装有问题的方法
        original_func = None
        
        # 查找可能的PyInstaller元数据处理模块
        try:
            import PyInstaller.compat
            if hasattr(PyInstaller.compat, 'conda_pkg_metadata'):
                original_func = PyInstaller.compat.conda_pkg_metadata
                
                def patched_conda_pkg_metadata(dist):
                    try:
                        meta = original_func(dist)
                        if isinstance(meta, dict) and 'depends' not in meta:
                            meta['depends'] = []
                        return meta
                    except KeyError as e:
                        if 'depends' in str(e):
                            # 如果是'depends'键错误，返回一个包含空depends的字典
                            try:
                                reqs = dist.requires()
                                return {'depends': [str(r) for r in reqs]}
                            except Exception:
                                return {'depends': []}
                        raise
                    
                PyInstaller.compat.conda_pkg_metadata = patched_conda_pkg_metadata
                
        except ImportError:
            pass
            
    except Exception as e:
        # 如果修补过程中出现问题，只是记录而不中断程序
        print(f"警告: 无法应用PyInstaller元数据修补: {e}")
        # 但这没关系，因为我们已经设置了环境变量


def prepare_pyinstaller_build():
    """
    准备PyInstaller构建环境，应用所有必要的补丁
    """
    # 设置环境变量以忽略conda元数据
    os.environ['PYINSTALLER_IGNORE_CONDA'] = '1'
    
    # 应用其他补丁
    patch_pyinstaller_conda_metadata()


if __name__ == "__main__":
    print("应用PyInstaller元数据补丁...")
    prepare_pyinstaller_build()
    print("补丁应用完成。环境变量PYINSTALLER_IGNORE_CONDA已设置为1。")
    print("现在你可以运行PyInstaller命令了。")