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
import java.util.Map.Entry;
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

import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataChangeEvent;

import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorUpdated;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeUpdated;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.OpendaylightInventoryListener;

public class ODLController implements DataChangeListener,
                                      OpendaylightInventoryListener,
                                      PacketProcessingListener,
                                      Controller {

  protected static final Logger LOG = LoggerFactory.getLogger(ODLController.class);

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

  private static final String LOCAL_PORT_STR = "LOCAL";

  /* Given from Activator. */

  private PacketProcessingService pps;

  private ODLController() {}

  public ODLController(PacketProcessingService pps) {
    this.pps = pps;
  }

  /* Implements DataChangeListener. */

  public void setDataStoreAccessor(FlowCommitWrapper dataStoreAccessor) {
    this.dataStoreAccessor = dataStoreAccessor;
  }

  /* Implements OpendaylightInventoryListener. */

  @Override
  public void onNodeConnectorRemoved(NodeConnectorRemoved notification) {
    NodeConnectorRef ncr = notification.getNodeConnectorRef();
    String portID = ncr
      .getValue()
      .firstIdentifierOf(NodeConnector.class)
      .firstKeyOf(NodeConnector.class, NodeConnectorKey.class)
      .getId()
      .getValue();

    if (portID.contains(LOCAL_PORT_STR))
      return;

    int portNum = portStrToInt(portID);
    port2NodeConnectorRef.remove(portNum);
    this.maple.portDown(portNum);

    System.out.println("NodeConnectorRef " + notification.getNodeConnectorRef());
  }

  @Override
  public void onNodeConnectorUpdated(NodeConnectorUpdated notification) {
    NodeConnectorRef ncr = notification.getNodeConnectorRef();
    String portID = ncr
      .getValue()
      .firstIdentifierOf(NodeConnector.class)
      .firstKeyOf(NodeConnector.class, NodeConnectorKey.class)
      .getId()
      .getValue();

    if (portID.contains(LOCAL_PORT_STR))
      return;

    int portNum = portStrToInt(portID);
    port2NodeConnectorRef.put(portNum, ncr);
    this.maple.portUp(portNum);

    System.out.println("NodeConnectorRef " + notification.getNodeConnectorRef());
  }

  @Override
  public void onNodeRemoved(NodeRemoved notification) {
    System.out.println("NodeRef " + notification.getNodeRef());
  }

  @Override
  public void onNodeUpdated(NodeUpdated notification) {
    System.out.println("NodeRef " + notification.getNodeRef());
  }

  @Override
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

    String portID = ingress
      .getValue()
      .firstIdentifierOf(NodeConnector.class)
      .firstKeyOf(NodeConnector.class, NodeConnectorKey.class)
      .getId()
      .getValue();

    int switchNum = switchStrToInt(portID);
    int portNum = portStrToInt(portID);

    MacAddress dstMac = PacketUtils.rawMacToMac(dstMacRaw);
    MacAddress srcMac = PacketUtils.rawMacToMac(srcMacRaw);
    System.out.println("Mapping portNum "+portNum+" to NodeConnectorRef. ");
    this.maple.handlePacket(data, switchNum, portNum);
  }

  private static int switchStrToInt(String switchStr) {
    return Integer.parseInt(switchStr.substring(
      switchStr.indexOf(':') + 1, switchStr.lastIndexOf(':')));
  }

  private static int portStrToInt(String portStr) {
    return Integer.parseInt(portStr.substring(
      portStr.lastIndexOf(':') + 1));
  }

  /**
   * starting controller
   */
  public void start() {
    LOG.debug("start() -->");

    this.maple = new MapleSystem(this);
    System.out.println("Maple Initiated");
    port2NodeConnectorRef = new HashMap<>();

    this.nodePath = InstanceIdentifierUtils.createNodePath(new NodeId("node_001"));

    LOG.debug("start() <--");
  }

  /**
   * stopping controller
   */
  public void stop() {
    LOG.debug("stop() -->");
    //TODO: remove flow (created in #start())
    LOG.debug("stop() <--");
  }
 
  private NodeConnectorRef ingressPlaceHolder(int portNum) {
    if (port2NodeConnectorRef.containsKey(portNum))
      return port2NodeConnectorRef.get(portNum);
    else
      throw new IllegalArgumentException("portNum " + portNum + " does not exist in map");
  }

  public synchronized void onSwitchAppeared(InstanceIdentifier<Table> appearedTablePath) {

    LOG.debug("expected table acquired, learning ..");

    // disable listening - simple learning handles only one node (switch)
/*
    if (registrationPublisher != null) {
      try {
        LOG.debug("closing dataChangeListenerRegistration");
        registrationPublisher.getDataChangeListenerRegistration().close();
      } catch (Exception e) {
        LOG.error("closing registration upon flowCapable node update listener failed: " + e.getMessage(), e);
      }
    }
*/

    tablePath = appearedTablePath;
    nodePath = tablePath.firstIdentifierOf(Node.class);
    nodeId = nodePath.firstKeyOf(Node.class, NodeKey.class).getId();
    mac2portMapping = new HashMap<>();
    coveredMacPaths = new HashSet<>();
/*
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
*/
  }

  @Override
  public void onDataChanged(AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> change) {
    Short requiredTableId = 0;

    Map<InstanceIdentifier<?>, DataObject> updated = change.getUpdatedData();
    for (Entry<InstanceIdentifier<?>, DataObject> updateItem : updated.entrySet()) {
      DataObject table = updateItem.getValue();
      if (table instanceof Table) {
        Table tableSure = (Table) table;
        LOG.trace("table: {}", table);

        if (requiredTableId.equals(tableSure.getId())) {
          @SuppressWarnings("unchecked")
          InstanceIdentifier<Table> tablePath = (InstanceIdentifier<Table>) updateItem.getKey();
          onSwitchAppeared(tablePath);
        }
      }
    }
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

  public void sendPacket(byte[] data, int inSwitch, int inPort, int... ports) {
    System.out.println("sendPacket Called in handler");

    if (ports[0] == Integer.MAX_VALUE) {
      flood(data, ingressPlaceHolder(inPort));
      return;
    }

    for (int i = 0; i < ports.length; i++) {
      NodeConnectorRef ncRef = PacketUtils.createNodeConnRef(
        nodePath,
        nodePath.firstKeyOf(Node.class, NodeKey.class),
        ports[i] + "");
      sendPacketOut(data, ingressPlaceHolder(inPort), ncRef);
    }

  }

  private void flood(byte[] payload, NodeConnectorRef ingress) {
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
    pps.transmitPacket(input);
  }
}
