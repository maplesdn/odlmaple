/**
 * Copyright (c) 2014 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package org.opendaylight.l2switch.maple;

import org.maple.core.Controller;
import org.maple.core.MapleSystem;

import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorUpdatedBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.Nodes;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.Table;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.Flow;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.FlowCookie;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnector;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnectorKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.TransmitPacketInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.TransmitPacketInputBuilder;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.opendaylight.controller.md.sal.binding.api.DataBroker;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.common.api.data.LogicalDatastoreType;
import org.opendaylight.controller.sal.binding.api.NotificationService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowCapableNode;
import org.opendaylight.yangtools.concepts.ListenerRegistration;
import org.opendaylight.yangtools.concepts.Registration;

public class ODLController implements DataChangeListenerRegistrationHolder,
                                      PacketProcessingListener,
                                      Controller {

  protected static final Logger LOG = LoggerFactory.getLogger(ODLController.class);

  private Registration packetInRegistration;

  private ListenerRegistration<DataChangeListener>
                                              dataChangeListenerRegistration;

  private static final byte[] ETH_TYPE_IPV4 = new byte[] { 0x08, 0x00 };

  private static final int DIRECT_FLOW_PRIORITY = 512;

  private MapleSystem maple;

  private FlowCommitWrapper dataStoreAccessor;

  private NodeId nodeId;
  private AtomicLong flowIdInc = new AtomicLong();
  private AtomicLong flowCookieInc = new AtomicLong(0x2a00000000000000L);

  private InstanceIdentifier<Node> nodePath;
  private InstanceIdentifier<Table> tablePath;

  private Map<MacAddress, NodeConnectorRef> mac2portMapping;
  private Set<String> coveredMacPaths;

  private Map<Integer, NodeConnectorRef> port2NodeConnectorRef;

  /* Given from Activator. */

  private NotificationService notificationService;
  private PacketProcessingService packetProcessingService;
  private DataBroker data;

  public void setDataBroker(DataBroker data) {
    this.data = data;
  }

  public void setPacketProcessingService(PacketProcessingService
                                         packetProcessingService) {
    this.packetProcessingService = packetProcessingService;
  }

  public void setNotificationService(NotificationService
                                     notificationService) {
    this.notificationService = notificationService;
  }

  /* Implements DataChangeListenerRegistrationHolder. */

  private DataChangeListenerRegistrationHolder registrationPublisher;

  public ListenerRegistration<DataChangeListener> 
                           getDataChangeListenerRegistration() {
    return dataChangeListenerRegistration;
  }

  /* Implements PacketProcessingListener. */

  public void onPacketReceived(PacketReceived packet) {
    if (packet == null || packet.getPayload() ==  null)
      return;

    byte[] data = packet.getPayload();

    LOG.debug("Received packet via match: {}", packet.getMatch());

    // read src MAC and dst MAC
    byte[] dstMacRaw = PacketUtils.extractDstMac(packet.getPayload());
    byte[] srcMacRaw = PacketUtils.extractSrcMac(packet.getPayload());
    byte[] etherType = PacketUtils.extractEtherType(packet.getPayload());

    NodeConnectorRef ingress = packet.getIngress();

    if (ingress == null)
      return;

    String switchID = ingress
        .getValue()
        .firstIdentifierOf(Node.class)
        .firstKeyOf(Node.class, NodeKey.class)
        .getId()
        .getValue();
    String portID = ingress
        .getValue()
        .firstIdentifierOf(NodeConnector.class)
        .firstKeyOf(NodeConnector.class, NodeConnectorKey.class)
        .getId()
        .getValue();

    int switchNum = Integer.parseInt(portID.substring(
        portID.indexOf(':') + 1, portID.lastIndexOf(':')));
    int portNum = Integer.parseInt(portID.substring(
        portID.lastIndexOf(':') + 1));

    MacAddress dstMac = PacketUtils.rawMacToMac(dstMacRaw);
    MacAddress srcMac = PacketUtils.rawMacToMac(srcMacRaw);
    port2NodeConnectorRef.put(portNum, ingress);
    System.out.println("Mapping portNum "+portNum+" to NodeConnectorRef. ");
    this.maple.handlePacket(data, switchNum, portNum);

/*
    NodeConnectorKey ingressKey = InstanceIdentifierUtils.getNodeConnectorKey(notification.getIngress().getValue());

      //LOG.debug("Received packet from MAC match: {}, ingress: {}", srcMac, ingressKey.getId());
      //LOG.debug("Received packet to   MAC match: {}", dstMac);
      //LOG.debug("Ethertype: {}", Integer.toHexString(0x0000ffff & ByteBuffer.wrap(etherType).getShort()));

      // learn by IPv4 traffic only
    if (Arrays.equals(ETH_TYPE_IPV4, etherType)) {
        NodeConnectorRef previousPort = mac2portMapping.put(srcMac, notification.getIngress());
        if (previousPort != null && !notification.getIngress().equals(previousPort)) {
            NodeConnectorKey previousPortKey = InstanceIdentifierUtils.getNodeConnectorKey(previousPort.getValue());
            LOG.debug("mac2port mapping changed by mac {}: {} -> {}", srcMac, previousPortKey, ingressKey.getId());
        }
        // if dst MAC mapped:
        NodeConnectorRef destNodeConnector = mac2portMapping.get(dstMac);
        if (destNodeConnector != null) {
            synchronized (coveredMacPaths) {
                if (!destNodeConnector.equals(notification.getIngress())) {
                    // add flow
                    addBridgeFlow(srcMac, dstMac, destNodeConnector);
                    addBridgeFlow(dstMac, srcMac, notification.getIngress());
                } else {
                    LOG.debug("useless rule ignoring - both MACs are behind the same port");
                }
            }
            LOG.debug("packetIn-directing.. to {}",
                    InstanceIdentifierUtils.getNodeConnectorKey(destNodeConnector.getValue()).getId());
            sendPacketOut(notification.getPayload(), notification.getIngress(), destNodeConnector);
        } else {
            // flood
            LOG.debug("packetIn-still flooding.. ");
            flood(notification.getPayload(), notification.getIngress());
        }
    } else {
        // non IPv4 package
        flood(notification.getPayload(), notification.getIngress());
    }
*/

  }

  /**
   * starting controller
   */
  public void start() {
    LOG.debug("start() -->");

    this.dataStoreAccessor = new FlowCommitWrapperImpl(data);
    this.maple = new MapleSystem(this);
    packetInRegistration = notificationService.registerNotificationListener(this);
    System.out.println("Maple Initiated");
    WakeupOnNode wakeupListener = new WakeupOnNode();
    wakeupListener.setController(this);
    port2NodeConnectorRef = new HashMap<>();
/*
    dataChangeListenerRegistration = data.registerDataChangeListener(LogicalDatastoreType.OPERATIONAL,
            InstanceIdentifier.builder(Nodes.class)
                .child(Node.class)
                .augmentation(FlowCapableNode.class)
                .child(Table.class).build(),
            wakeupListener,
            DataBroker.DataChangeScope.SUBTREE);
*/
    LOG.debug("start() <--");
  }

  /**
   * stopping controller
   */
  public void stop() {
    LOG.debug("stop() -->");
    //TODO: remove flow (created in #start())
    try {
      packetInRegistration.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    try {
      dataChangeListenerRegistration.close();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
      LOG.debug("stop() <--");
  }
 
    // similar to flood(), create a NodeConnectorRef as a placeholder for ingress
  private NodeConnectorRef ingressPlaceHolder(int portNum) {
    if (port2NodeConnectorRef.get(portNum) != null) {
      return port2NodeConnectorRef.get(portNum);
    } else {
      NodeConnectorUpdatedBuilder ncub = new NodeConnectorUpdatedBuilder();
      NodeConnectorId nodeConnectorId = new NodeConnectorId("portName");

      NodeId nodeId = new NodeId("node_001");
      InstanceIdentifier<NodeConnector> instanceIdentifier = InstanceIdentifier.builder(Nodes.class)
                  .child(Node.class, new NodeKey(nodeId))
                  .child(NodeConnector.class, new NodeConnectorKey(nodeConnectorId)).toInstance();
      NodeConnectorRef nodeConnectorRef = new NodeConnectorRef(instanceIdentifier);
      return nodeConnectorRef;
    }
  }

  public synchronized void onSwitchAppeared(InstanceIdentifier<Table> appearedTablePath) {

    LOG.debug("expected table acquired, learning ..");

    // disable listening - simple learning handles only one node (switch)
    if (registrationPublisher != null) {
      try {
        LOG.debug("closing dataChangeListenerRegistration");
        registrationPublisher.getDataChangeListenerRegistration().close();
      } catch (Exception e) {
        LOG.error("closing registration upon flowCapable node update listener failed: " + e.getMessage(), e);
      }
    }

    tablePath = appearedTablePath;
    nodePath = tablePath.firstIdentifierOf(Node.class);
    nodeId = nodePath.firstKeyOf(Node.class, NodeKey.class).getId();
    mac2portMapping = new HashMap<>();
    coveredMacPaths = new HashSet<>();

    // start forwarding all packages to controller
    FlowId flowId = new FlowId(String.valueOf(flowIdInc.getAndIncrement()));
    FlowKey flowKey = new FlowKey(flowId);
    InstanceIdentifier<Flow> flowPath = InstanceIdentifierUtils.createFlowPath(tablePath, flowKey);

    int priority = 0;
    // create flow in table with id = 0, priority = 4 (other params are
    // defaulted in OFDataStoreUtil)
    FlowBuilder allToCtrlFlow = FlowUtils.createFwdAllToControllerFlow(
            InstanceIdentifierUtils.getTableId(tablePath), priority, flowId);

    LOG.debug("writing packetForwardToController flow");
    dataStoreAccessor.writeFlowToConfig(flowPath, allToCtrlFlow.build());
  }

  /**
   * @param srcMac
   * @param dstMac
   * @param destNodeConnector
   */
  private void addBridgeFlow(MacAddress srcMac, MacAddress dstMac, NodeConnectorRef destNodeConnector) {
    synchronized (coveredMacPaths) {
      String macPath = srcMac.toString() + dstMac.toString();
      if (!coveredMacPaths.contains(macPath)) {
        LOG.debug("covering mac path: {} by [{}]", macPath,
                  destNodeConnector.getValue().firstKeyOf(NodeConnector.class, NodeConnectorKey.class).getId());

        coveredMacPaths.add(macPath);
        FlowId flowId = new FlowId(String.valueOf(flowIdInc.getAndIncrement()));
        FlowKey flowKey = new FlowKey(flowId);
        /* Path to the flow we want to program. */
        InstanceIdentifier<Flow> flowPath = InstanceIdentifierUtils.createFlowPath(tablePath, flowKey);

        Short tableId = InstanceIdentifierUtils.getTableId(tablePath);
        FlowBuilder srcToDstFlow = FlowUtils.createDirectMacToMacFlow(tableId, DIRECT_FLOW_PRIORITY, srcMac,
                      dstMac, destNodeConnector);
        srcToDstFlow.setCookie(new FlowCookie(BigInteger.valueOf(flowCookieInc.getAndIncrement())));

        dataStoreAccessor.writeFlowToConfig(flowPath, srcToDstFlow.build());
      }
    }
  }

  public void sendPacket(byte[] data, int... ports) {
    System.out.println("sendPacket Called in handler");

    if (ports[0] == Integer.MAX_VALUE) {
        int MAGIC_PORT_NUM = 1; // TODO: fill this in properly eventually.
      flood(data, ingressPlaceHolder(MAGIC_PORT_NUM));
      return;
    }

    for (int i = 0; i < ports.length; i++) {
        NodeConnectorRef ncRef = PacketUtils.createNodeConnRef(
                nodePath,
                nodePath.firstKeyOf(Node.class, NodeKey.class),
                ports[i] + "");
        //System.out.println("sendPacket.ncRef: " + ncRef);
        sendPacketOut(data, ingressPlaceHolder(ports[i]), ncRef);
    }

  }

  private void flood(byte[] payload, NodeConnectorRef ingress) {
    // System.out.println("trying to flood the switch, but nodePath is not defined");
    // return;

      NodeConnectorKey nodeConnectorKey = new NodeConnectorKey(nodeConnectorId("0xfffffffb"));
      InstanceIdentifier<?> nodeConnectorPath = InstanceIdentifierUtils.createNodeConnectorPath(nodePath, nodeConnectorKey);
      NodeConnectorRef egressConnectorRef = new NodeConnectorRef(nodeConnectorPath);

      sendPacketOut(payload, ingress, egressConnectorRef);
  }

  private NodeConnectorId nodeConnectorId(String connectorId) {
    NodeKey nodeKey = nodePath.firstKeyOf(Node.class, NodeKey.class);
    StringBuilder stringId = new StringBuilder(nodeKey.getId().getValue()).append(":").append(connectorId);
    return new NodeConnectorId(stringId.toString());
  }

  private void sendPacketOut(byte[] payload, NodeConnectorRef ingress, NodeConnectorRef egress) {
    InstanceIdentifier<Node> egressNodePath = InstanceIdentifierUtils.getNodePath(egress.getValue());
    TransmitPacketInput input = new TransmitPacketInputBuilder()
              .setPayload(payload)
              .setNode(new NodeRef(egressNodePath))
              .setEgress(egress)
              .setIngress(ingress)
              .build();
    packetProcessingService.transmitPacket(input);
  }
}
