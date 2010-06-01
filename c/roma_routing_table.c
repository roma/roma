/* 
 * File:   roma_sender.c
 * Author: yosuke hara
 *
 * Created on 2009/06/29
 */

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <math.h>

#include "sha1/sha1.h"
#include "yaml/yaml.h"
#include "roma_client_private.h"
#include "roma_common.h"

#define RMC_YAML_KEY     0
#define RMC_YAML_VALUE   1
#define RMC_YAML_SEQ_ON  0
#define RMC_YAML_SEQ_OFF 1

#define RMC_RD_DGST_BITS "dgst_bits"
#define RMC_RD_DIV_BITS  "div_bits"
#define RMC_RD_NODES     "nodes"
#define RMC_RD_RN        "rn"
#define RMC_RD_V_CLK     "v_clk"
#define RMC_RD_V_IDX     "v_idx"

#define RMC_DEFAULT_NODES 3
#define RMC_DEFAULT_CLKS  1024

enum rmc_key_type {
    KEY_DGST_BITS = 0,
    KEY_DIV_BITS  = 1,
    KEY_NODES     = 2,
    KEY_RN        = 3,
    KEY_V_CLK     = 4,
    KEY_V_IDX     = 5,
    KEY_UNDEFINED = 99
};

// routing-table.
//rmc_routing_table rmc_rtable;

/**
 * create routing table.
 * @param[in] routing data
 */
int rmc_create_routing_table(rmc_routing_data *rd)
{
    rd->hbits = 4294967296L;
    rd->mklhash = NULL;
    /*
    rd->fail_count =
        (rmc_fail_count *)calloc(rd->number_of_nodes, sizeof(rmc_fail_count));
    int i;
    for (i = 0; i < rd->number_of_nodes; i++)
    {
        //char *node = (char *)calloc(strlen(nodes[i]), sizeof(char));
        //strcpy(node, nodes[i]);

        rd->fail_count[i].node = nodes[i];
        rd->fail_count[i].count = 0;
    }
    
    printf("             rn:%d\n",  rd->rn);
    printf("      dgst_bits:%d\n",  rd->dgst_bits);
    printf("       div_bits:%d\n",  rd->div_bits);
    printf("          hbits:%Lu\n", rd->hbits);
    printf("    search_mask:%lu\n", rd->search_mask);
    printf("number_of_nodes:%d\n",  rd->number_of_nodes);
    printf("          nodes:\n");
    int i;
    for (i =  0; i < rd->number_of_nodes; i++)
    printf("            [%s]\n", rd->nodes[i].node);
    printf("-----\n");
     */
}

/**
 * check routing data.
 * @param rouing dump yaml
 */
int rmc_check_routing_data(char *routing_dump_yaml)
{
    yaml_parser_t parser;
    yaml_event_t event;
    int done = 0;
    int count = 0;
    int error = 0;

    assert(yaml_parser_initialize(&parser));
    yaml_parser_set_input_string(&parser, routing_dump_yaml, strlen(routing_dump_yaml));

    while (!done)
    {
        if (!yaml_parser_parse(&parser, &event)) {
            error = 1;
            break;
        }
        done = (event.type == YAML_STREAM_END_EVENT);
        yaml_event_delete(&event);
        count ++;
    }
    yaml_parser_delete(&parser);

    //printf("%s (%d events)\n", (error ? "FAILURE" : "SUCCESS"), count);
    return 0;
}

/**
 * generate routing data.
 *
 * @param[in]  routing_dump_yaml
 * @param[out] rd - routing-data
 * @return 
 *
 */
rmc_routing_data rmc_generate_routing_data(const char *routing_dump_yaml, const int nodes)
{
    rmc_routing_data rd;

    yaml_parser_t parser;
    yaml_event_t event;
    int done = 0;

    char *token = (char *)calloc(strlen(routing_dump_yaml), sizeof(char));
    strcpy(token, routing_dump_yaml);

    assert(yaml_parser_initialize(&parser));
    yaml_parser_set_input_string(&parser, token, strlen(token));

    int is_key = RMC_YAML_KEY;
    int level = 0, is_seq = RMC_YAML_SEQ_OFF, key_index, seq_index;
    int key_type;

    rd.nodes = (rmc_node *)calloc(nodes, sizeof(rmc_node)); // @TODO
    rd.v_idx = (rmc_virtual_node *)calloc(RMC_DEFAULT_CLKS, sizeof(rmc_virtual_node));

    while (!done)
    {
        if (!yaml_parser_parse(&parser, &event)){
            yaml_parser_delete(&parser);
            //free(token);
            return (rd);
        }
        done = (event.type == YAML_STREAM_END_EVENT);

        if (event.type == YAML_SCALAR_EVENT)
        {
            char *scalar_value = event.data.scalar.value;
            if      (rmc_index_of_string(scalar_value, RMC_RD_DGST_BITS) == 0) key_type = KEY_DGST_BITS;
            else if (rmc_index_of_string(scalar_value, RMC_RD_DIV_BITS ) == 0) key_type = KEY_DIV_BITS;
            else if (rmc_index_of_string(scalar_value, RMC_RD_NODES    ) == 0) key_type = KEY_NODES;
            else if (rmc_index_of_string(scalar_value, RMC_RD_RN       ) == 0) key_type = KEY_RN;
            else if (rmc_index_of_string(scalar_value, RMC_RD_V_CLK    ) == 0) key_type = KEY_V_CLK;
            else if (rmc_index_of_string(scalar_value, RMC_RD_V_IDX    ) == 0) key_type = KEY_V_IDX;

            if (is_seq == RMC_YAML_SEQ_OFF)
            {
                if (is_key == RMC_YAML_KEY)
                {
                    if (key_type == KEY_V_IDX)
                    {
                        rd.v_idx[key_index].index =
                            (unsigned long)strtoul(scalar_value, (char **)NULL, 0);
                    }
                    key_index++;
                }
                else
                {
                    switch (key_type)
                    {
                        case KEY_DGST_BITS:
                            rd.dgst_bits = atoi(scalar_value);
                            break;
                        case KEY_DIV_BITS:
                            rd.div_bits = atoi(scalar_value);
                            break;
                        case KEY_RN:
                            rd.rn = atoi(scalar_value);
                            break;
                        case KEY_NODES:
                        case KEY_V_IDX:
                        default:
                            break;
                    }
                }
                is_key++;
            }
            else
            {                
                if (key_type == KEY_NODES)
                {
                    rd.nodes[seq_index].node =
                        (char *)calloc(RMC_DEFAULT_CLKS, sizeof(char)); 
                    strcpy(rd.nodes[seq_index].node, scalar_value);
                }
                else if (key_type == KEY_V_IDX)
                {
                    rd.v_idx[key_index-1].nodes[seq_index].node =
                        (char *)calloc(RMC_DEFAULT_CLKS, sizeof(char));
                    strcpy(rd.v_idx[key_index-1].nodes[seq_index].node, scalar_value);
                }
                seq_index++;
            }
        }
        else if (event.type == YAML_SEQUENCE_START_EVENT)
        {
            is_seq = RMC_YAML_SEQ_ON;
            seq_index = 0;

            if (key_type == KEY_V_IDX)
            {
                rd.v_idx[key_index-1].nodes =
                    (rmc_node *)calloc(nodes, sizeof(rmc_node));
            }
        }
        else if (event.type == YAML_SEQUENCE_END_EVENT)
        {
            is_seq = RMC_YAML_SEQ_OFF;
            is_key = RMC_YAML_KEY;
        }
        else if (event.type == YAML_MAPPING_START_EVENT)
        {
            is_key = RMC_YAML_KEY;
            key_index = 0;
            level++;
        }
        else if (event.type == YAML_MAPPING_END_EVENT)
        {
            is_key = RMC_YAML_KEY;
            level--;
        }
        yaml_event_delete(&event);
        if (is_key > RMC_YAML_VALUE) is_key = RMC_YAML_KEY;
    }

    if (rd.dgst_bits > 0 && rd.div_bits > 0)
    {
        //printf("****search mask:[%d]\n", rd.div_bits);
        //double div_bits = (double)rd.div_bits;
        rd.search_mask = (long)(pow(2, 9) -1);
        rd.search_mask = rd.search_mask<<(rd.dgst_bits-rd.div_bits);
    }
    
    yaml_parser_delete(&parser);
    //free(token);
    return (rd);
};

/**
 * check fail count.
 * @param rd - rounting data
 * @param node
 * @return [OK|NG]
 
int _check_fail_count(rmc_routing_data rd, const char *node)
{
    int i, result = 0;
    for (i = 0; i < rd.number_of_nodes; i++)
    {        
        if (strcmp(node, rd.fail_count[i].node) == 0)
        {
            result = rd.fail_count[i].count;
            break;
        }
    }
    return (result);
}
 */

/**
 * roma-client: search node.
 * @param[in]  key
 * @param[in]  rd - routing data.
 * @return digest and node.
 *
 */
rmc_digest_node rmc_search_node(const char *key, rmc_routing_data rd)
{
    SHA1Context sha;
    SHA1Reset(&sha);
    SHA1Input(&sha, (const unsigned char *) key, strlen(key));
    rmc_digest_node dn;

    if (!SHA1Result(&sha))
        return dn;
    else
    {
        unsigned int digest =(sha.Message_Digest[4] & 0xffffffff);
        unsigned int v_idx  =(digest & rd.search_mask)>>(rd.dgst_bits-rd.div_bits);

        int node_idx = 0;
        /*
        int i, j;
        for (i = 0; ; i++)
        {
            if (rd.v_idx[v_idx].nodes[i].node == NULL) break;
            int ret = _check_fail_count(rd, rd.v_idx[v_idx].nodes[i].node);           
            if (ret == 0)
            {
                printf("\n===> RET-NODE: [%s]\n", rd.v_idx[v_idx].nodes[i].node);
                node_idx = i;
                break;
            }
        }
         */

        dn.digest = digest;
        dn.node = rd.v_idx[v_idx].nodes[node_idx].node;
    }
    return (dn);
}
