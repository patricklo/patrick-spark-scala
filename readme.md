#scala jar打包配置
####本地引入scala-sdk-2.11.4.jar 和 spark-assembly-1.3.0-hadoop2.4.0
##File -> Project Structure -> Artifacts
  包上面两个包加进来，然后输入主函数cn.spark.study.core.WordCount
  
#打包
Build -> build artifacts


#上传命令
scp test.txt roo@spark1:/tmp/

