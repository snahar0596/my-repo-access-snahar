import os


def calculate_monthly_cost(total_input_tokens, total_output_tokens, input_price_per_1k=0.00135, output_price_per_1k=0.0054):
    """
    Calculate direct cost for used input and output tokens, (token consumption)
    Returns: Dictionary containing detailed cost breakdown
    """

    price_per_input_token = input_price_per_1k / 1000
    price_per_output_token = output_price_per_1k / 1000


    # Calculate costs
    input_cost = total_input_tokens * price_per_input_token
    output_cost = total_output_tokens * price_per_output_token
    total_cost = input_cost + output_cost


    # Return detailed breakdown
    return {
        'total_input_tokens': total_input_tokens,
        'total_output_tokens': total_output_tokens,
        'input_price_per_1k': input_price_per_1k,
        'output_price_per_1k': output_price_per_1k,
        'price_per_input_token': price_per_input_token,
        'price_per_output_token': price_per_output_token,
        'input_cost_usd': input_cost,
        'output_cost_usd': output_cost,
        'total_cost_usd': total_cost
    }


def format_cost_breakdown(results):
    """Format the cost breakdown in a readable way"""

    print("\n" + "="*60)
    print("=" * 60)
    print("SIMPLE DIRECT COST CALCULATION BREAKDOWN")
    print("=" * 60)
    print(f"Total input tokens: {results['total_input_tokens']}")
    print(f"Total output tokens: {results['total_output_tokens']}")
    print(f"Input price per 1k: {results['input_price_per_1k']:,.6f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Output price per 1k: {results['output_price_per_1k']:,.6f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print()
    print(f"Price per input token: {results['price_per_input_token']:,.10f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Price per output token: {results['price_per_output_token']:,.10f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print()
    print(f"Input cost usd: ${results['input_cost_usd']:,.6f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"Output cost usd: ${results['output_cost_usd']:,.6f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"TOTAL COST USD: {results['total_cost_usd']:,.6f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print()
    print("=" * 60)
    print("=" * 60)


if __name__ == "__main__":

    want_to_continue = True

    while(bool(want_to_continue) == True):
        os.system('cls')
        total_input_tokens = float(eval(input('total_input_tokens:  ')))
        total_output_tokens = float(eval(input('total_output_tokens:  ')))
        input_price_per_1k = float(eval(input('input_price_per_1k, (0.00135):  ')))
        output_price_per_1k = float(eval(input('output_price_per_1k, (0.0054):  ')))


        results = calculate_monthly_cost(
            total_input_tokens=total_input_tokens,
            total_output_tokens=total_output_tokens,
            input_price_per_1k=input_price_per_1k,
            output_price_per_1k=output_price_per_1k,
        )

        format_cost_breakdown(results)

        want_to_continue = str(input('\n\nDo you want to calculate again?:  '))
