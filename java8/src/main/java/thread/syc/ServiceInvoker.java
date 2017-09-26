package thread.syc;


import thread.util.Debug;

public class ServiceInvoker {
  // 保存当前类的唯一实例
  private static final ServiceInvoker INSTANCE = new ServiceInvoker();
  // 负载均衡器实例，使用volatile变量保障可见性
  private volatile LoadBalancer loadBalancer;

  // 私有构造器
  private ServiceInvoker() {
    // 什么也不做
  }

  /**
   * 获取当前类的唯一实例
   */
  public static ServiceInvoker getInstance() {
    return INSTANCE;
  }

  /**
   * 根据指定的负载均衡器派发请求到特定的下游部件。
   *
   * @param request
   *          待派发的请求
   */
  public void dispatchRequest(Request request) {
    // 这里读取volatile变量loadBalancer
    Endpoint endpoint = getLoadBalancer().nextEndpoint();

    if (null == endpoint) {
      // 省略其他代码

      return;
    }

    // 将请求发给下游部件
    dispatchToDownstream(request, endpoint);

  }

  // 真正将指定的请求派发给下游部件
  private void dispatchToDownstream(Request request, Endpoint endpoint) {
    Debug.info("Dispatch request to " + endpoint + ":" + request);
    // 省略其他代码
  }

  public LoadBalancer getLoadBalancer() {
    // 读取负载均衡器实例
    return loadBalancer;
  }

  public void setLoadBalancer(LoadBalancer loadBalancer) {
    // 设置或者更新负载均衡器实例
    this.loadBalancer = loadBalancer;
  }
}
